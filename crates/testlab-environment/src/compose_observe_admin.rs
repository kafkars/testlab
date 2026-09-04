//! Compose records one immediate independent observation after an eligible admin terminal.

use std::time::{Duration, Instant};

use testlab_schema::{
    AdapterCommand, EnvironmentOperation, EnvironmentOperationKind, EnvironmentOperationStatus,
    ScenarioAction,
};

use crate::compose::DockerComposeEnvironment;
use crate::compose_support::elapsed_unix_ms;
use crate::compose_types::{ComposeObservation, ComposePhase};
use crate::observer_admin::{AdminObserverRequest, capture};
use crate::observer_admin_target::AdminTarget;
use crate::observer_error::ObserverError;

impl DockerComposeEnvironment {
    /// Independently captures broker state after one correlated admin terminal.
    pub fn observe_admin(
        &mut self,
        action: &ScenarioAction,
        command: &AdapterCommand,
        timeout: Duration,
    ) -> ComposeObservation {
        let target = match AdminTarget::from_exact(action, command) {
            Ok(Some(target)) => Ok(target),
            Ok(None) => return ComposeObservation::default(),
            Err(error) => Err(error),
        };
        if self.cluster_size > 1
            && let Ok(target) = &target
            && crate::group_cli_observation::supports(target)
        {
            return self.observe_groups_with_cli(target, timeout);
        }
        let mut phase = ComposePhase::default();
        let id = match self.operation_id() {
            Ok(id) => id,
            Err(error) => {
                phase.fail(error.code(), error.diagnostic());
                return empty(phase);
            }
        };
        let endpoint = self.endpoint();
        let operation_started = Instant::now();
        let started_unix_ms = elapsed_unix_ms(self.started_unix_ms, self.started.elapsed());
        let (args, result) = match target {
            Ok(target) => {
                let mut args = vec!["--bootstrap-server".to_owned(), endpoint.clone()];
                args.extend(target.args());
                let result = operation_started
                    .checked_add(timeout)
                    .ok_or(ObserverError::DeadlineOverflow)
                    .and_then(|deadline| {
                        let first_observation = self.begin_admin_observation(&target)?;
                        capture(
                            AdminObserverRequest {
                                endpoint: &endpoint,
                                run_id: &self.run_id,
                                deadline,
                                security: &self.client_security,
                                cluster_size: self.cluster_size,
                                first_observation,
                            },
                            &target,
                        )
                    });
                (args, result)
            }
            Err(error) => (
                vec![
                    "--bootstrap-server".to_owned(),
                    endpoint,
                    "--correlation".to_owned(),
                    "rejected".to_owned(),
                ],
                Err(error),
            ),
        };
        let completed_unix_ms = elapsed_unix_ms(started_unix_ms, operation_started.elapsed());
        let (status, diagnostic, state_observations) = match result {
            Ok(observations) => (EnvironmentOperationStatus::Succeeded, None, observations),
            Err(error) => {
                let status = if error.is_timeout() {
                    EnvironmentOperationStatus::TimedOut
                } else {
                    EnvironmentOperationStatus::Failed
                };
                let diagnostic = error.to_string();
                phase.fail("environment_observation_failed", &diagnostic);
                (status, Some(diagnostic), Vec::new())
            }
        };
        let (_, librdkafka_version) = rdkafka::util::get_rdkafka_version();
        phase.operations.push(EnvironmentOperation {
            id,
            kind: EnvironmentOperationKind::BrokerObserve,
            program: format!("librdkafka/{librdkafka_version}"),
            args,
            started_unix_ms,
            completed_unix_ms,
            status,
            exit_code: None,
            stdout_artifact: None,
            stderr_artifact: None,
            diagnostic,
        });
        ComposeObservation {
            phase,
            observations: Vec::new(),
            state_observations,
        }
    }

    pub(super) fn begin_admin_observation(
        &mut self,
        target: &AdminTarget,
    ) -> Result<u64, ObserverError> {
        if !self
            .observed_admin_operations
            .insert(target.operation_id().clone())
        {
            return Err(ObserverError::InvalidTarget(format!(
                "admin operation {} was already observed",
                target.operation_id()
            )));
        }
        let first = self.next_state_observation;
        let count = u64::try_from(target.observation_count())
            .map_err(|_| ObserverError::ObservationOverflow)?;
        self.next_state_observation = first
            .checked_add(count)
            .ok_or(ObserverError::ObservationOverflow)?;
        Ok(first)
    }
}

fn empty(phase: ComposePhase) -> ComposeObservation {
    ComposeObservation {
        phase,
        observations: Vec::new(),
        state_observations: Vec::new(),
    }
}
