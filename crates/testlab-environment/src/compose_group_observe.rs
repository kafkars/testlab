//! Multi-broker group snapshots use the pinned Kafka admin CLI and retain raw evidence.

use std::time::{Duration, Instant};

use testlab_schema::EnvironmentOperationKind;

use crate::compose::DockerComposeEnvironment;
use crate::compose_command::compose_owned;
use crate::compose_support::remaining;
use crate::compose_types::ComposeObservation;
use crate::group_cli_observation;
use crate::observer_admin_target::AdminTarget;

impl DockerComposeEnvironment {
    pub(super) fn observe_groups_with_cli(
        &mut self,
        target: &AdminTarget,
        timeout: Duration,
    ) -> ComposeObservation {
        let mut observed = ComposeObservation::default();
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            observed.phase.fail(
                "environment_observation_failed",
                "group observation deadline overflow",
            );
            return observed;
        };
        let first = match self.begin_admin_observation(target) {
            Ok(first) => first,
            Err(error) => {
                observed
                    .phase
                    .fail("environment_observation_failed", error.to_string());
                return observed;
            }
        };
        let Some(service) = self.broker_services.first() else {
            observed.phase.fail(
                "environment_observation_failed",
                "no broker service for group query",
            );
            return observed;
        };
        let mut args = vec![
            "exec".to_owned(),
            "--no-TTY".to_owned(),
            service.clone(),
            "/opt/kafka/bin/kafka-consumer-groups.sh".to_owned(),
            "--bootstrap-server".to_owned(),
            format!("localhost:{}", self.client_port),
            "--timeout".to_owned(),
            remaining(deadline).as_millis().to_string(),
            "--describe".to_owned(),
            "--state".to_owned(),
        ];
        args.extend(group_cli_observation::selection(target));
        let operation = self.next_operation;
        let spec = compose_owned(
            EnvironmentOperationKind::BrokerObserve,
            &self.prefix,
            args,
            format!("group-state-{operation:05}.txt"),
            format!("group-state-{operation:05}.stderr.txt"),
        );
        let output = match self.execute(spec, remaining(deadline)) {
            Ok(output) => output,
            Err(error) => {
                observed.phase.fail(error.code, error.diagnostic);
                return observed;
            }
        };
        let result = group_cli_observation::normalize(first, target, &output.stdout);
        if observed.phase.retain(output) {
            match result {
                Ok(states) => observed.state_observations = states,
                Err(error) => observed
                    .phase
                    .fail("environment_observation_failed", error.to_string()),
            }
        } else {
            observed.phase.fail(
                "environment_observation_failed",
                "Kafka CLI group query failed",
            );
        }
        observed
    }
}
