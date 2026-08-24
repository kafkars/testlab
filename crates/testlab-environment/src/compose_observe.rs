//! Compose observation records one bounded independent Kafka snapshot operation.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use testlab_schema::{
    EnvironmentOperation, EnvironmentOperationKind, EnvironmentOperationStatus,
    ListConsumerGroupOffsetsCommand, OperationId, Scenario,
};

use crate::compose::DockerComposeEnvironment;
use crate::compose_support::elapsed_unix_ms;
use crate::compose_types::{ComposeObservation, ComposePhase};
use crate::observer::{ObserverRequest, capture as capture_records};
use crate::observer_error::ObserverError;
use crate::observer_group_offset::capture as capture_group_offsets;

impl DockerComposeEnvironment {
    /// Snapshots issued record and broker-state targets independently of the subject.
    pub fn observe(
        &mut self,
        scenario: &Scenario,
        issued_record_operations: &BTreeSet<OperationId>,
        issued_group_offset_commands: &[ListConsumerGroupOffsetsCommand],
        timeout: Duration,
    ) -> ComposeObservation {
        let mut phase = ComposePhase::default();
        let id = match self.operation_id() {
            Ok(id) => id,
            Err(error) => {
                phase.fail(error.code(), error.diagnostic());
                return ComposeObservation {
                    phase,
                    observations: Vec::new(),
                    state_observations: Vec::new(),
                };
            }
        };
        let endpoint = self.endpoint();
        let operation_started = Instant::now();
        let started_unix_ms = elapsed_unix_ms(self.started_unix_ms, self.started.elapsed());
        let deadline = operation_started.checked_add(timeout);
        let request = |deadline| ObserverRequest {
            endpoint: &endpoint,
            run_id: &self.run_id,
            scenario,
            issued_record_operations,
            issued_group_offset_commands,
            deadline,
            security: &self.client_security,
        };
        let record_result = deadline
            .ok_or(ObserverError::DeadlineOverflow)
            .and_then(|deadline| capture_records(request(deadline)));
        let (observations, state_result) = match record_result {
            Ok(observations) => {
                let result = deadline
                    .ok_or(ObserverError::DeadlineOverflow)
                    .and_then(|deadline| capture_group_offsets(request(deadline)));
                (observations, result)
            }
            Err(error) => (Vec::new(), Err(error)),
        };
        let completed_unix_ms = elapsed_unix_ms(started_unix_ms, operation_started.elapsed());
        let (status, diagnostic, state_observations) = match state_result {
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
            args: vec![
                "--bootstrap-server".to_owned(),
                endpoint,
                "--scenario".to_owned(),
                scenario.id.to_string(),
            ],
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
            observations,
            state_observations,
        }
    }
}
