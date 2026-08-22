//! Compose observation records one bounded independent Kafka snapshot operation.

use std::time::{Duration, Instant};

use testlab_schema::{
    EnvironmentOperation, EnvironmentOperationKind, EnvironmentOperationStatus, Scenario,
};

use crate::compose::DockerComposeEnvironment;
use crate::compose_support::elapsed_unix_ms;
use crate::compose_types::{ComposeObservation, ComposePhase};
use crate::observer::{ObserverRequest, capture};

impl DockerComposeEnvironment {
    /// Snapshots all scenario topic-partitions with a client independent of the subject.
    pub fn observe(&mut self, scenario: &Scenario, timeout: Duration) -> ComposeObservation {
        let mut phase = ComposePhase::default();
        let id = match self.operation_id() {
            Ok(id) => id,
            Err(error) => {
                phase.fail(error.code(), error.diagnostic());
                return ComposeObservation {
                    phase,
                    observations: Vec::new(),
                };
            }
        };
        let endpoint = self.endpoint();
        let operation_started = Instant::now();
        let started_unix_ms = elapsed_unix_ms(self.started_unix_ms, self.started.elapsed());
        let result = capture(ObserverRequest {
            endpoint: &endpoint,
            run_id: &self.run_id,
            scenario,
            timeout,
        });
        let completed_unix_ms = elapsed_unix_ms(started_unix_ms, operation_started.elapsed());
        let (status, diagnostic, observations) = match result {
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
        }
    }
}
