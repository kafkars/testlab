//! Cluster feature snapshots use Kafka's pinned CLI and stable numeric mappings.

use std::time::{Duration, Instant};

use testlab_schema::EnvironmentOperationKind;

use crate::compose::DockerComposeEnvironment;
use crate::compose_command::compose_owned;
use crate::compose_support::remaining;
use crate::compose_types::ComposeObservation;
use crate::feature_cli_observation;
use crate::observer_admin_target::AdminTarget;

impl DockerComposeEnvironment {
    pub(super) fn observe_features_with_cli(
        &mut self,
        target: &AdminTarget,
        timeout: Duration,
    ) -> ComposeObservation {
        let mut observed = ComposeObservation::default();
        let AdminTarget::Features(operation_id) = target else {
            observed.phase.fail(
                "environment_observation_failed",
                "non-feature target reached feature observer",
            );
            return observed;
        };
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            observed.phase.fail(
                "environment_observation_failed",
                "feature observation deadline overflow",
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
        let Some(service) = self.broker_services.first().cloned() else {
            observed.phase.fail(
                "environment_observation_failed",
                "no broker service for feature query",
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
                "/opt/kafka/bin/kafka-features.sh".to_owned(),
                "--bootstrap-server".to_owned(),
                format!("localhost:{}", self.client_port),
                "describe".to_owned(),
            ],
            format!("feature-state-{operation:05}.txt"),
            format!("feature-state-{operation:05}.stderr.txt"),
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
                "Kafka CLI feature query failed",
            );
            return observed;
        }
        match feature_cli_observation::normalize(observation, operation_id, &stdout) {
            Ok(state) => observed.state_observations.push(state),
            Err(error) => observed
                .phase
                .fail("environment_observation_failed", error.to_string()),
        }
        observed
    }
}
