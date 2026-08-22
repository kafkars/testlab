//! Compose execution assigns stable identities before supervising terminal effects.

use std::time::Duration;

use testlab_schema::EnvironmentOperationId;

use crate::compose::DockerComposeEnvironment;
use crate::compose_command::CommandSpec;
use crate::compose_support::elapsed_unix_ms;
use crate::compose_types::ComposeFailure;
use crate::{TerminalOutput, TerminalRequest, run_terminal};

impl DockerComposeEnvironment {
    pub(super) fn execute(
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

    pub(super) fn operation_id(&mut self) -> Result<EnvironmentOperationId, ComposeFailure> {
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
