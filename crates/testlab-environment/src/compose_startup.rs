//! Compose startup owns one evidenced retry after an ephemeral host-port collision.

use std::time::{Duration, Instant};

use testlab_schema::{EnvironmentOperationKind, EnvironmentOperationStatus};

use crate::compose::DockerComposeEnvironment;
use crate::compose_command::{self, CommandSpec};
use crate::compose_support::remaining;
use crate::compose_types::ComposePhase;

const PORT_COLLISION_RECOVERY_LIMIT: u8 = 1;

impl DockerComposeEnvironment {
    /// Starts the pinned image and waits for every declared broker service.
    pub fn start(&mut self, timeout: Duration) -> ComposePhase {
        let mut phase = ComposePhase::default();
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            phase.fail("environment_deadline_invalid", "setup deadline overflowed");
            return phase;
        };
        let image = self.environment[0].1.clone();
        if !self.required(&mut phase, compose_command::image_pull(&image), deadline)
            || !self.required(&mut phase, compose_command::image_inspect(&image), deadline)
            || !self.required(&mut phase, compose_command::config(&self.prefix), deadline)
        {
            return phase;
        }
        self.release_compose_ports();
        self.up_attempted = true;
        if !self.start_project(&mut phase, deadline) {
            return phase;
        }
        for service in self.broker_services.clone() {
            if !self.wait_ready(&mut phase, &service, deadline) {
                return phase;
            }
        }
        if !self.prepare_broker_features(&mut phase, deadline)
            || !self.prepare_client_security(&mut phase, deadline)
            || !self.start_network_proxy(&mut phase, deadline)
        {
            return phase;
        }
        phase
    }

    fn start_project(&mut self, phase: &mut ComposePhase, deadline: Instant) -> bool {
        let first = match self.execute(compose_command::up(&self.prefix), remaining(deadline)) {
            Ok(output) => output,
            Err(error) => {
                phase.fail(error.code, error.diagnostic);
                return false;
            }
        };
        let diagnostic = first.operation.diagnostic.clone();
        let port_collision = is_host_port_collision(&first);
        if phase.retain(first) {
            return true;
        }
        if !port_collision {
            phase.fail(
                "environment_compose_up_failed",
                diagnostic.unwrap_or_else(|| "environment_compose_up_failed".to_owned()),
            );
            return false;
        }
        self.recover_host_ports(phase, deadline, PORT_COLLISION_RECOVERY_LIMIT)
    }

    fn recover_host_ports(
        &mut self,
        phase: &mut ComposePhase,
        deadline: Instant,
        attempt: u8,
    ) -> bool {
        if !self.required(phase, recovery_down(&self.prefix, attempt), deadline) {
            return false;
        }
        if let Err(error) = self.reassign_compose_ports() {
            phase.fail(error.code, error.diagnostic);
            return false;
        }
        if !self.required(phase, recovery_config(&self.prefix, attempt), deadline) {
            return false;
        }
        self.release_compose_ports();
        self.required(phase, recovery_up(&self.prefix, attempt), deadline)
    }
}

fn is_host_port_collision(output: &crate::TerminalOutput) -> bool {
    output.operation.status == EnvironmentOperationStatus::Failed
        && output.operation.diagnostic.as_deref().is_some_and(|value| {
            value.contains("failed to bind port") && value.contains("address already in use")
        })
}

fn recovery_down(prefix: &[String], attempt: u8) -> CommandSpec {
    recovery_command(
        EnvironmentOperationKind::ComposeDown,
        prefix,
        &["down", "--volumes", "--remove-orphans"],
        "down",
        attempt,
    )
}

fn recovery_config(prefix: &[String], attempt: u8) -> CommandSpec {
    recovery_command(
        EnvironmentOperationKind::ComposeConfig,
        prefix,
        &["config"],
        "config",
        attempt,
    )
}

fn recovery_up(prefix: &[String], attempt: u8) -> CommandSpec {
    recovery_command(
        EnvironmentOperationKind::ComposeUp,
        prefix,
        &["up", "--detach", "--no-build", "--remove-orphans"],
        "up",
        attempt,
    )
}

fn recovery_command(
    kind: EnvironmentOperationKind,
    prefix: &[String],
    tail: &[&str],
    phase: &str,
    attempt: u8,
) -> CommandSpec {
    compose_command::compose_owned(
        kind,
        prefix,
        tail.iter().map(|value| (*value).to_owned()).collect(),
        format!("compose-port-recovery-{phase}-{attempt:03}.txt"),
        format!("compose-port-recovery-{phase}-{attempt:03}.stderr.txt"),
    )
}
