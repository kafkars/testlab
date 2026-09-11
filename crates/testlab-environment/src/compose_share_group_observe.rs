//! Share-group snapshots use Kafka's pinned type-specific administration CLI.

use std::time::{Duration, Instant};

use testlab_schema::EnvironmentOperationKind;

use crate::compose::DockerComposeEnvironment;
use crate::compose_command::compose_owned;
use crate::compose_support::remaining;
use crate::compose_types::ComposeObservation;
use crate::observer_admin_target::AdminTarget;

impl DockerComposeEnvironment {
    pub(super) fn observe_share_group_with_cli(
        &mut self,
        target: &AdminTarget,
        timeout: Duration,
    ) -> ComposeObservation {
        let mut observed = ComposeObservation::default();
        let (selection, artifact) = match target {
            AdminTarget::ShareGroup(target) => (
                vec![
                    "--describe".to_owned(),
                    "--state".to_owned(),
                    "--group".to_owned(),
                    target.group_id.clone(),
                ],
                "state",
            ),
            AdminTarget::ShareGroups(_) => (vec!["--list".to_owned()], "list"),
            AdminTarget::ShareGroupOffset(target) => (
                vec![
                    "--describe".to_owned(),
                    "--offsets".to_owned(),
                    "--group".to_owned(),
                    target.group_id.clone(),
                ],
                "offsets",
            ),
            _ => {
                observed.phase.fail(
                    "environment_observation_failed",
                    "non-Share-group target reached Share-group observer",
                );
                return observed;
            }
        };
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            observed.phase.fail(
                "environment_observation_failed",
                "Share-group observation deadline overflow",
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
        let Some(service) = self.broker_services.first().cloned() else {
            observed.phase.fail(
                "environment_observation_failed",
                "no broker service for Share-group query",
            );
            return observed;
        };
        let operation = self.next_operation;
        let mut args = vec![
            "exec".to_owned(),
            "--no-TTY".to_owned(),
            service,
            "/opt/kafka/bin/kafka-share-groups.sh".to_owned(),
            "--bootstrap-server".to_owned(),
            format!("localhost:{}", self.client_port),
            "--timeout".to_owned(),
            remaining(deadline).as_millis().to_string(),
        ];
        args.extend(selection);
        let spec = compose_owned(
            EnvironmentOperationKind::BrokerObserve,
            &self.prefix,
            args,
            format!("share-group-{artifact}-{operation:05}.txt"),
            format!("share-group-{artifact}-{operation:05}.stderr.txt"),
        );
        let output = match self.execute(spec, remaining(deadline)) {
            Ok(output) => output,
            Err(error) => {
                observed.phase.fail(error.code, error.diagnostic);
                return observed;
            }
        };
        let result = match target {
            AdminTarget::ShareGroups(target) => {
                crate::share_group_cli_observation::normalize_list(first, target, &output.stdout)
            }
            _ => crate::share_group_cli_observation::normalize(first, target, &output.stdout)
                .map(|observation| vec![observation]),
        };
        if observed.phase.retain(output) {
            match result {
                Ok(states) => observed.state_observations.extend(states),
                Err(error) => observed
                    .phase
                    .fail("environment_observation_failed", error.to_string()),
            }
        } else {
            observed.phase.fail(
                "environment_observation_failed",
                "Kafka CLI Share-group query failed",
            );
        }
        observed
    }
}
