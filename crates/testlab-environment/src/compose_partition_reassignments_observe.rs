//! Active partition reassignments are captured through Kafka's pinned CLI.

use std::time::{Duration, Instant};

use testlab_schema::EnvironmentOperationKind;

use crate::compose::DockerComposeEnvironment;
use crate::compose_command::compose_owned;
use crate::compose_support::remaining;
use crate::compose_types::ComposeObservation;
use crate::observer_admin_target::AdminTarget;

impl DockerComposeEnvironment {
    pub(super) fn observe_partition_reassignments_with_cli(
        &mut self,
        target: &AdminTarget,
        timeout: Duration,
    ) -> ComposeObservation {
        let mut observed = ComposeObservation::default();
        let AdminTarget::PartitionReassignments(target) = target else {
            observed.phase.fail(
                "environment_observation_failed",
                "non-reassignment target reached reassignment CLI observer",
            );
            return observed;
        };
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            observed.phase.fail(
                "environment_observation_failed",
                "reassignment observation deadline overflow",
            );
            return observed;
        };
        let observation = match self
            .begin_admin_observation(&AdminTarget::PartitionReassignments(target.clone()))
        {
            Ok(value) => value,
            Err(error) => {
                observed
                    .phase
                    .fail("environment_observation_failed", error.to_string());
                return observed;
            }
        };
        let Some(service) = self.broker_services.first().cloned() else {
            observed.phase.fail(
                "environment_observation_failed",
                "no broker service for partition-reassignment query",
            );
            return observed;
        };
        let operation = self.next_operation;
        let spec = compose_owned(
            EnvironmentOperationKind::BrokerObserve,
            &self.prefix,
            vec![
                "exec".to_owned(),
                "--no-TTY".to_owned(),
                service,
                "/opt/kafka/bin/kafka-reassign-partitions.sh".to_owned(),
                "--bootstrap-server".to_owned(),
                format!("localhost:{}", self.client_port),
                "--list".to_owned(),
            ],
            format!("partition-reassignments-{operation:05}.txt"),
            format!("partition-reassignments-{operation:05}.stderr.txt"),
        );
        let output = match self.execute(spec, remaining(deadline)) {
            Ok(value) => value,
            Err(error) => {
                observed.phase.fail(error.code, error.diagnostic);
                return observed;
            }
        };
        let stdout = output.stdout.clone();
        if !observed.phase.retain(output) {
            observed.phase.fail(
                "environment_observation_failed",
                "Kafka CLI partition-reassignment query failed",
            );
            return observed;
        }
        match crate::partition_reassignments_cli_observation::normalize(
            observation,
            &target.operation_id,
            &stdout,
        ) {
            Ok(state) => observed.state_observations.push(state),
            Err(error) => observed
                .phase
                .fail("environment_observation_failed", error.to_string()),
        }
        observed
    }
}
