//! Metadata-quorum snapshots retain both official Kafka CLI views.

use std::time::{Duration, Instant};

use testlab_schema::EnvironmentOperationKind;

use crate::TerminalOutput;
use crate::compose::DockerComposeEnvironment;
use crate::compose_command::compose_owned;
use crate::compose_support::remaining;
use crate::compose_types::{ComposeFailure, ComposeObservation};
use crate::observer_admin_target::AdminTarget;

impl DockerComposeEnvironment {
    pub(super) fn observe_metadata_quorum_with_cli(
        &mut self,
        target: &AdminTarget,
        timeout: Duration,
    ) -> ComposeObservation {
        let mut observed = ComposeObservation::default();
        let AdminTarget::MetadataQuorum(operation_id) = target else {
            observed.phase.fail(
                "environment_observation_failed",
                "non-quorum target reached metadata-quorum observer",
            );
            return observed;
        };
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            observed.phase.fail(
                "environment_observation_failed",
                "metadata-quorum observation deadline overflow",
            );
            return observed;
        };
        let observation = match self.begin_admin_observation(target) {
            Ok(value) => value,
            Err(error) => {
                observed
                    .phase
                    .fail("environment_observation_failed", error.to_string());
                return observed;
            }
        };
        let status = match self.execute_metadata_quorum_cli("status", deadline) {
            Ok(output) => output,
            Err(error) => {
                observed.phase.fail(error.code, error.diagnostic);
                return observed;
            }
        };
        let status_stdout = status.stdout.clone();
        if !observed.phase.retain(status) {
            observed.phase.fail(
                "environment_observation_failed",
                "Kafka CLI metadata-quorum status query failed",
            );
            return observed;
        }
        let replication = match self.execute_metadata_quorum_cli("replication", deadline) {
            Ok(output) => output,
            Err(error) => {
                observed.phase.fail(error.code, error.diagnostic);
                return observed;
            }
        };
        let replication_stdout = replication.stdout.clone();
        if !observed.phase.retain(replication) {
            observed.phase.fail(
                "environment_observation_failed",
                "Kafka CLI metadata-quorum replication query failed",
            );
            return observed;
        }
        match crate::metadata_quorum_cli_observation::normalize(
            observation,
            operation_id,
            &status_stdout,
            &replication_stdout,
        ) {
            Ok(state) => observed.state_observations.push(state),
            Err(error) => observed
                .phase
                .fail("environment_observation_failed", error.to_string()),
        }
        observed
    }

    fn execute_metadata_quorum_cli(
        &mut self,
        view: &str,
        deadline: Instant,
    ) -> Result<TerminalOutput, ComposeFailure> {
        let service = self.broker_services.first().cloned().ok_or_else(|| {
            ComposeFailure::new(
                "environment_observation_failed",
                "no broker service for metadata-quorum query",
            )
        })?;
        let operation = self.next_operation;
        self.execute(
            compose_owned(
                EnvironmentOperationKind::BrokerObserve,
                &self.prefix,
                vec![
                    "exec".to_owned(),
                    "--no-TTY".to_owned(),
                    service,
                    "/opt/kafka/bin/kafka-metadata-quorum.sh".to_owned(),
                    "--bootstrap-server".to_owned(),
                    format!("localhost:{}", self.client_port),
                    "describe".to_owned(),
                    format!("--{view}"),
                ],
                format!("metadata-quorum-{view}-{operation:05}.txt"),
                format!("metadata-quorum-{view}-{operation:05}.stderr.txt"),
            ),
            remaining(deadline),
        )
    }
}
