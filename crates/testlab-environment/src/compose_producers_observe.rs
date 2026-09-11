//! Active-producer snapshots use Kafka's pinned administration CLI.

use std::time::{Duration, Instant};

use testlab_schema::EnvironmentOperationKind;

use crate::compose::DockerComposeEnvironment;
use crate::compose_command::compose_owned;
use crate::compose_support::remaining;
use crate::compose_types::ComposeObservation;
use crate::observer_admin_target::AdminTarget;
use crate::producer_cli_observation;

impl DockerComposeEnvironment {
    pub(super) fn observe_producers_with_cli(
        &mut self,
        target: &AdminTarget,
        timeout: Duration,
    ) -> ComposeObservation {
        let mut observed = ComposeObservation::default();
        let AdminTarget::Producers(target) = target else {
            observed.phase.fail(
                "environment_observation_failed",
                "non-producer target reached active-producer observer",
            );
            return observed;
        };
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            observed.phase.fail(
                "environment_observation_failed",
                "active-producer observation deadline overflow",
            );
            return observed;
        };
        let observation =
            match self.begin_admin_observation(&AdminTarget::Producers(target.clone())) {
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
                "no broker service for active-producer query",
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
                "/opt/kafka/bin/kafka-transactions.sh".to_owned(),
                "--bootstrap-server".to_owned(),
                format!("localhost:{}", self.client_port),
                "describe-producers".to_owned(),
                "--topic".to_owned(),
                target.topic.clone(),
                "--partition".to_owned(),
                target.partition.to_string(),
            ],
            format!("producer-state-{operation:05}.txt"),
            format!("producer-state-{operation:05}.stderr.txt"),
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
                "Kafka CLI active-producer query failed",
            );
            return observed;
        }
        match producer_cli_observation::normalize(observation, target, &stdout) {
            Ok(state) => observed.state_observations.push(state),
            Err(error) => observed
                .phase
                .fail("environment_observation_failed", error.to_string()),
        }
        observed
    }
}
