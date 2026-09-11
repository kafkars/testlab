//! Client-quota snapshots use the pinned Kafka CLI and retain the raw terminal.

use std::time::{Duration, Instant};

use testlab_schema::EnvironmentOperationKind;

use crate::client_quota_cli_observation;
use crate::compose::DockerComposeEnvironment;
use crate::compose_command::compose_owned;
use crate::compose_support::remaining;
use crate::compose_types::ComposeObservation;
use crate::observer_admin_target::AdminTarget;

impl DockerComposeEnvironment {
    pub(super) fn observe_client_quota_with_cli(
        &mut self,
        target: &AdminTarget,
        timeout: Duration,
    ) -> ComposeObservation {
        let mut observed = ComposeObservation::default();
        let AdminTarget::ClientQuota(quota) = target else {
            observed.phase.fail(
                "environment_observation_failed",
                "non-client-quota target reached client-quota observer",
            );
            return observed;
        };
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            observed.phase.fail(
                "environment_observation_failed",
                "client-quota observation deadline overflow",
            );
            return observed;
        };
        let observation = match self.begin_admin_observation(target) {
            Ok(observation) => observation,
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
                "no broker service for client-quota query",
            );
            return observed;
        };
        let mut args = vec![
            "exec".to_owned(),
            "--no-TTY".to_owned(),
            service,
            "/opt/kafka/bin/kafka-configs.sh".to_owned(),
            "--bootstrap-server".to_owned(),
            format!("localhost:{}", self.client_port),
            "--describe".to_owned(),
        ];
        args.extend(client_quota_cli_observation::selection(&quota.user));
        let operation = self.next_operation;
        let spec = compose_owned(
            EnvironmentOperationKind::BrokerObserve,
            &self.prefix,
            args,
            format!("client-quota-state-{operation:05}.txt"),
            format!("client-quota-state-{operation:05}.stderr.txt"),
        );
        let output = match self.execute(spec, remaining(deadline)) {
            Ok(output) => output,
            Err(error) => {
                observed.phase.fail(error.code, error.diagnostic);
                return observed;
            }
        };
        let result = client_quota_cli_observation::normalize(
            observation,
            &quota.operation_id,
            &quota.user,
            quota.direction,
            &output.stdout,
        );
        if !observed.phase.retain(output) {
            observed.phase.fail(
                "environment_observation_failed",
                "Kafka CLI client-quota query failed",
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
