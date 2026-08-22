//! Docker Compose lifecycle owns immutable setup, readiness, snapshots, and cleanup.

use std::fmt::{Debug, Formatter};
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use testlab_schema::{AdapterSecurity, EnvironmentDriver, RunId, TransportSecurity};

use crate::compose_command::{self, CommandSpec};
use crate::compose_ports::HostPorts;
use crate::compose_support::{compose_prefix, failure_code, project_name, remaining};
use crate::compose_types::{ComposeFailure, ComposePhase, ComposeRequest};
use crate::security::ClientSecurity;

const READINESS_ATTEMPT_MAX: Duration = Duration::from_secs(5);
const READINESS_RETRY_DELAY: Duration = Duration::from_millis(250);

/// One isolated Compose project that must be explicitly finished for evidence.
#[must_use = "call finish so owned containers, networks, and volumes are removed"]
pub struct DockerComposeEnvironment {
    pub(super) repository_root: PathBuf,
    pub(super) run_id: RunId,
    pub(super) program: PathBuf,
    pub(super) prefix: Vec<String>,
    pub(super) environment: Vec<(String, String)>,
    pub(super) client_security: ClientSecurity,
    pub(super) broker_services: Vec<String>,
    pub(super) client_port: u16,
    pub(super) cluster_size: u16,
    host_ports: HostPorts,
    pub(super) started_unix_ms: u64,
    pub(super) started: Instant,
    pub(super) next_operation: u32,
    up_attempted: bool,
}

impl Debug for DockerComposeEnvironment {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DockerComposeEnvironment")
            .field("run_id", &self.run_id)
            .field("project", &self.prefix.get(2))
            .field("host_ports", &self.host_ports.as_slice())
            .field("up_attempted", &self.up_attempted)
            .finish_non_exhaustive()
    }
}

impl DockerComposeEnvironment {
    /// Resolves an immutable manifest into a side-effect-free lifecycle owner.
    pub fn new(request: ComposeRequest<'_>) -> Result<Self, ComposeFailure> {
        request.environment.validate().map_err(|error| {
            ComposeFailure::new("environment_manifest_invalid", error.to_string())
        })?;
        let EnvironmentDriver::DockerCompose {
            broker_services, ..
        } = &request.environment.driver
        else {
            return Err(ComposeFailure::new(
                "environment_driver_invalid",
                "environment does not use the Docker Compose driver",
            ));
        };
        let host_ports = HostPorts::reserve(broker_services.len())?;
        Self::with_program(request, PathBuf::from("docker"), host_ports)
    }

    #[cfg(test)]
    pub(crate) fn new_with_program(
        request: ComposeRequest<'_>,
        program: PathBuf,
        host_port: u16,
    ) -> Result<Self, ComposeFailure> {
        let count = match &request.environment.driver {
            EnvironmentDriver::DockerCompose {
                broker_services, ..
            } => broker_services.len(),
            EnvironmentDriver::ModelBroker => 0,
        };
        Self::with_program(request, program, HostPorts::fixed(host_port, count)?)
    }

    fn with_program(
        request: ComposeRequest<'_>,
        program: PathBuf,
        host_ports: HostPorts,
    ) -> Result<Self, ComposeFailure> {
        request.environment.validate().map_err(|error| {
            ComposeFailure::new("environment_manifest_invalid", error.to_string())
        })?;
        let EnvironmentDriver::DockerCompose {
            image,
            cluster_size,
            security,
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
        let security_directory = (security.transport == TransportSecurity::TlsCustom).then(|| {
            request
                .repository_root
                .join("target/testlab-security")
                .join(request.run_id.as_str())
        });
        let ca_pem = security_directory.as_ref().map(|path| path.join("ca.pem"));
        let client_security = ClientSecurity::new(*security, ca_pem.as_deref())?;
        let environment = client_security.compose_environment(
            image,
            host_ports.as_slice(),
            security_directory.as_deref(),
        );
        Ok(Self {
            repository_root: request.repository_root.to_path_buf(),
            run_id: request.run_id.clone(),
            program,
            prefix,
            environment,
            client_security,
            broker_services: broker_services.clone(),
            client_port: *client_port,
            cluster_size: *cluster_size,
            host_ports,
            started_unix_ms: request.started_unix_ms,
            started: Instant::now(),
            next_operation: 0,
            up_attempted: false,
        })
    }

    /// Returns the loopback bootstrap endpoint advertised to the adapter.
    pub fn endpoint(&self) -> String {
        self.host_ports.endpoint()
    }

    /// Returns every independently reachable bootstrap endpoint in broker order.
    pub fn endpoints(&self) -> Vec<String> {
        self.host_ports.endpoints()
    }

    /// Returns the non-secret connection policy sent in the adapter handshake.
    pub fn adapter_security(&self) -> AdapterSecurity {
        self.client_security.adapter_security()
    }

    /// Returns ephemeral secret values passed only in the adapter process environment.
    pub fn adapter_environment(&self) -> Vec<(String, String)> {
        self.client_security.adapter_environment()
    }

    /// Starts the pinned image and waits for every declared broker service.
    pub fn start(&mut self, timeout: Duration) -> ComposePhase {
        let mut phase = ComposePhase::default();
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            phase.fail("environment_deadline_invalid", "setup deadline overflowed");
            return phase;
        };
        let image = self.environment[0].1.clone();
        if !self.required(&mut phase, compose_command::image_pull(&image), deadline) {
            return phase;
        }
        if !self.required(&mut phase, compose_command::image_inspect(&image), deadline) {
            return phase;
        }
        if !self.required(&mut phase, compose_command::config(&self.prefix), deadline) {
            return phase;
        }
        self.host_ports.release();
        self.up_attempted = true;
        if !self.required(&mut phase, compose_command::up(&self.prefix), deadline) {
            return phase;
        }
        for service in self.broker_services.clone() {
            if !self.wait_ready(&mut phase, &service, deadline) {
                return phase;
            }
        }
        if !self.prepare_client_security(&mut phase, deadline) {
            return phase;
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

    pub(super) fn required(
        &mut self,
        phase: &mut ComposePhase,
        spec: CommandSpec,
        deadline: Instant,
    ) -> bool {
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
}
