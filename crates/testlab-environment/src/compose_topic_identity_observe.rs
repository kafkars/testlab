//! Topic-ID descriptions retain one pinned Kafka CLI query per caller-ordered topic.

use std::time::{Duration, Instant};

use testlab_schema::EnvironmentOperationKind;

use crate::compose::DockerComposeEnvironment;
use crate::compose_command::compose_owned;
use crate::compose_support::remaining;
use crate::compose_types::ComposeObservation;
use crate::observer_admin_target::{AdminTarget, ordinal};

impl DockerComposeEnvironment {
    pub(super) fn observe_topic_identities_with_cli(
        &mut self,
        target: &AdminTarget,
        timeout: Duration,
    ) -> ComposeObservation {
        let mut observed = ComposeObservation::default();
        let AdminTarget::TopicIdentities(target) = target else {
            observed.phase.fail(
                "environment_observation_failed",
                "non-topic-identity target reached topic-identity observer",
            );
            return observed;
        };
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            observed.phase.fail(
                "environment_observation_failed",
                "topic-identity observation deadline overflow",
            );
            return observed;
        };
        let first =
            match self.begin_admin_observation(&AdminTarget::TopicIdentities(target.clone())) {
                Ok(first) => first,
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
                "no broker service for topic-identity query",
            );
            return observed;
        };
        for (index, topic) in target.names.iter().enumerate() {
            let observation = match ordinal(first, index) {
                Ok(value) => value,
                Err(error) => {
                    observed
                        .phase
                        .fail("environment_observation_failed", error.to_string());
                    return observed;
                }
            };
            let operation = self.next_operation;
            let spec = compose_owned(
                EnvironmentOperationKind::BrokerObserve,
                &self.prefix,
                vec![
                    "exec".to_owned(),
                    "--no-TTY".to_owned(),
                    service.clone(),
                    "/opt/kafka/bin/kafka-topics.sh".to_owned(),
                    "--bootstrap-server".to_owned(),
                    format!("localhost:{}", self.client_port),
                    "--describe".to_owned(),
                    "--topic".to_owned(),
                    topic.clone(),
                ],
                format!("topic-identity-{operation:05}.txt"),
                format!("topic-identity-{operation:05}.stderr.txt"),
            );
            let output = match self.execute(spec, remaining(deadline)) {
                Ok(value) => value,
                Err(error) => {
                    observed.phase.fail(error.code, error.diagnostic);
                    return observed;
                }
            };
            let normalized = crate::topic_identity_cli_observation::normalize(
                observation,
                &target.operation_id,
                topic,
                &output.stdout,
            );
            let succeeded = output.succeeded();
            observed.phase.retain(output);
            if !succeeded {
                observed.phase.fail(
                    "environment_observation_failed",
                    "Kafka CLI topic-identity query failed",
                );
                return observed;
            }
            match normalized {
                Ok(state) => observed.state_observations.push(state),
                Err(error) => {
                    observed
                        .phase
                        .fail("environment_observation_failed", error.to_string());
                    return observed;
                }
            }
            if remaining(deadline).is_zero() && index + 1 < target.names.len() {
                observed.phase.fail(
                    "environment_observation_failed",
                    "plural topic-identity observation deadline elapsed",
                );
                return observed;
            }
        }
        observed
    }
}
