//! Client-metrics resource snapshots use Kafka's pinned CLI and retain raw output.

use std::time::{Duration, Instant};

use testlab_schema::EnvironmentOperationKind;

use crate::client_metrics_resources_cli_observation;
use crate::compose::DockerComposeEnvironment;
use crate::compose_command::compose_owned;
use crate::compose_support::remaining;
use crate::compose_types::ComposeObservation;
use crate::observer_admin_target::AdminTarget;

impl DockerComposeEnvironment {
    pub(super) fn observe_client_metrics_resources_with_cli(
        &mut self,
        target: &AdminTarget,
        timeout: Duration,
    ) -> ComposeObservation {
        let mut observed = ComposeObservation::default();
        let AdminTarget::ClientMetricsResources(resources) = target else {
            observed.phase.fail(
                "environment_observation_failed",
                "non-client-metrics target reached client-metrics resource observer",
            );
            return observed;
        };
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            observed.phase.fail(
                "environment_observation_failed",
                "client-metrics resource observation deadline overflow",
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
                "no broker service for client-metrics resource query",
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
                "/opt/kafka/bin/kafka-client-metrics.sh".to_owned(),
                "--bootstrap-server".to_owned(),
                format!("localhost:{}", self.client_port),
                "--list".to_owned(),
            ],
            format!("client-metrics-resources-{operation:05}.txt"),
            format!("client-metrics-resources-{operation:05}.stderr.txt"),
        );
        let output = match self.execute(spec, remaining(deadline)) {
            Ok(value) => value,
            Err(error) => {
                observed.phase.fail(error.code, error.diagnostic);
                return observed;
            }
        };
        let result = client_metrics_resources_cli_observation::normalize(
            observation,
            &resources.operation_id,
            &output.stdout,
        );
        if !observed.phase.retain(output) {
            observed.phase.fail(
                "environment_observation_failed",
                "Kafka CLI client-metrics resource query failed",
            );
            return observed;
        }
        match result {
            Ok(state) => observed.state_observations.push(state),
            Err(error) => observed
                .phase
                .fail("environment_observation_failed", error.to_string()),
        }
        observed
    }
}
