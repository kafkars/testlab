//! Docker Compose lifecycle owns immutable setup, readiness, snapshots, and cleanup.

use std::fmt::{Debug, Formatter};
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use testlab_schema::{EnvironmentDriver, EnvironmentOperationId, RunId};

use crate::compose_command::{self, CommandSpec};
use crate::compose_support::{
    compose_prefix, elapsed_unix_ms, failure_code, project_name, remaining,
};
use crate::compose_types::{ComposeFailure, ComposePhase, ComposeRequest};
use crate::{TerminalOutput, TerminalRequest, run_terminal};

const READINESS_ATTEMPT_MAX: Duration = Duration::from_secs(5);
const READINESS_RETRY_DELAY: Duration = Duration::from_millis(250);

/// One isolated Compose project that must be explicitly finished for evidence.
#[must_use = "call finish so owned containers, networks, and volumes are removed"]
pub struct DockerComposeEnvironment {
    repository_root: PathBuf,
    run_id: RunId,
    program: PathBuf,
    prefix: Vec<String>,
    environment: Vec<(String, String)>,
    broker_services: Vec<String>,
    client_port: u16,
    host_port: u16,
    started_unix_ms: u64,
    started: Instant,
    next_operation: u32,
    up_attempted: bool,
}

impl Debug for DockerComposeEnvironment {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DockerComposeEnvironment")
            .field("run_id", &self.run_id)
            .field("project", &self.prefix.get(2))
            .field("host_port", &self.host_port)
            .field("up_attempted", &self.up_attempted)
            .finish_non_exhaustive()
    }
}

impl DockerComposeEnvironment {
    /// Resolves an immutable manifest into a side-effect-free lifecycle owner.
    pub fn new(request: ComposeRequest<'_>) -> Result<Self, ComposeFailure> {
        Self::with_program(request, PathBuf::from("docker"))
    }

    #[cfg(test)]
    pub(crate) fn new_with_program(
        request: ComposeRequest<'_>,
        program: PathBuf,
    ) -> Result<Self, ComposeFailure> {
        Self::with_program(request, program)
    }

    fn with_program(request: ComposeRequest<'_>, program: PathBuf) -> Result<Self, ComposeFailure> {
        request.environment.validate().map_err(|error| {
            ComposeFailure::new("environment_manifest_invalid", error.to_string())
        })?;
        if request.host_port == 0 {
            return Err(ComposeFailure::new(
                "environment_host_port_invalid",
                "host port must be greater than zero",
            ));
        }
        let EnvironmentDriver::DockerCompose {
            image,
            compose_files,
            broker_services,
            client_port,
            ..
        } = &request.environment.driver
        else {
            return Err(ComposeFailure::new(
                "environment_driver_invalid",
                "environment does not use the Docker Compose driver",
            ));
        };
        let project = project_name(request.run_id);
        let prefix = compose_prefix(&project, compose_files);
        Ok(Self {
            repository_root: request.repository_root.to_path_buf(),
            run_id: request.run_id.clone(),
            program,
            prefix,
            environment: vec![
                ("IMAGE".to_owned(), image.clone()),
                ("KAFKA_HOST_PORT".to_owned(), request.host_port.to_string()),
            ],
            broker_services: broker_services.clone(),
            client_port: *client_port,
            host_port: request.host_port,
            started_unix_ms: request.started_unix_ms,
            started: Instant::now(),
            next_operation: 0,
            up_attempted: false,
        })
    }

    /// Returns the loopback bootstrap endpoint advertised to the adapter.
    pub fn endpoint(&self) -> String {
        format!("127.0.0.1:{}", self.host_port)
    }

    /// Starts the pinned image and waits for every declared broker service.
    pub fn start(&mut self, timeout: Duration) -> ComposePhase {
        let mut phase = ComposePhase::default();
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            phase.fail("environment_deadline_invalid", "setup deadline overflowed");
            return phase;
        };
        let image = self.environment[0].1.clone();
        if !self.required(&mut phase, compose_command::image_inspect(&image), deadline) {
            return phase;
        }
        if !self.required(&mut phase, compose_command::config(&self.prefix), deadline) {
            return phase;
        }
        self.up_attempted = true;
        if !self.required(&mut phase, compose_command::up(&self.prefix), deadline) {
            return phase;
        }
        for service in self.broker_services.clone() {
            if !self.wait_ready(&mut phase, &service, deadline) {
                return phase;
            }
        }
        phase
    }

    /// Captures final state and logs before removing all project resources.
    pub fn finish(mut self, timeout: Duration) -> ComposePhase {
        let mut phase = ComposePhase::default();
        if !self.up_attempted {
            return phase;
        }
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            phase.fail(
                "environment_cleanup_deadline_invalid",
                "cleanup deadline overflowed",
            );
            return phase;
        };
        let commands = [
            (
                compose_command::ps(&self.prefix),
                "environment_snapshot_failed",
            ),
            (
                compose_command::logs(&self.prefix, &self.broker_services),
                "environment_logs_failed",
            ),
            (
                compose_command::down(&self.prefix),
                "environment_cleanup_failed",
            ),
        ];
        for (index, (spec, code)) in commands.into_iter().enumerate() {
            let remaining_commands = u32::try_from(3 - index).unwrap_or(1);
            let timeout = remaining(deadline) / remaining_commands;
            match self.execute(spec, timeout) {
                Ok(output) => {
                    if !phase.retain(output) {
                        phase.fail(code, format!("{code} terminal operation failed"));
                    }
                }
                Err(error) => phase.fail(error.code, error.diagnostic),
            }
        }
        phase
    }

    fn required(&mut self, phase: &mut ComposePhase, spec: CommandSpec, deadline: Instant) -> bool {
        let code = failure_code(spec.kind);
        match self.execute(spec, remaining(deadline)) {
            Ok(output) => {
                let diagnostic = output.operation.diagnostic.clone();
                if phase.retain(output) {
                    true
                } else {
                    phase.fail(code, diagnostic.unwrap_or_else(|| format!("{code} failed")));
                    false
                }
            }
            Err(error) => {
                phase.fail(error.code, error.diagnostic);
                false
            }
        }
    }

    fn wait_ready(&mut self, phase: &mut ComposePhase, service: &str, deadline: Instant) -> bool {
        let mut attempt = 1_u32;
        loop {
            let timeout = remaining(deadline).min(READINESS_ATTEMPT_MAX);
            let spec = compose_command::readiness(&self.prefix, service, self.client_port, attempt);
            match self.execute(spec, timeout) {
                Ok(output) => {
                    if phase.retain(output) {
                        return true;
                    }
                }
                Err(error) => {
                    phase.fail(error.code, error.diagnostic);
                    return false;
                }
            }
            if remaining(deadline).is_zero() {
                phase.fail(
                    "environment_readiness_failed",
                    format!("broker service {service} did not become ready"),
                );
                return false;
            }
            thread::sleep(READINESS_RETRY_DELAY.min(remaining(deadline)));
            attempt = if let Some(value) = attempt.checked_add(1) {
                value
            } else {
                phase.fail(
                    "environment_operation_overflow",
                    "readiness attempt overflowed",
                );
                return false;
            };
        }
    }

    fn execute(
        &mut self,
        spec: CommandSpec,
        timeout: Duration,
    ) -> Result<TerminalOutput, ComposeFailure> {
        let id = self.operation_id()?;
        Ok(run_terminal(TerminalRequest {
            id,
            kind: spec.kind,
            program: self.program.display().to_string(),
            args: spec.args,
            current_directory: self.repository_root.clone(),
            environment: self.environment.clone(),
            started_unix_ms: elapsed_unix_ms(self.started_unix_ms, self.started.elapsed()),
            timeout,
            stdout_artifact: Some(spec.stdout_artifact),
            stderr_artifact: Some(spec.stderr_artifact),
        }))
    }

    fn operation_id(&mut self) -> Result<EnvironmentOperationId, ComposeFailure> {
        let sequence = self.next_operation;
        self.next_operation = self.next_operation.checked_add(1).ok_or_else(|| {
            ComposeFailure::new(
                "environment_operation_overflow",
                "operation sequence overflowed",
            )
        })?;
        EnvironmentOperationId::new(format!("{}:environment:{sequence:05}", self.run_id)).map_err(
            |error| ComposeFailure::new("environment_operation_id_invalid", error.to_string()),
        )
    }
}
