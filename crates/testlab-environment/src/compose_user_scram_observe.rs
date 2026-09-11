//! User SCRAM snapshots use the pinned Kafka CLI and retain the raw terminal.

use std::time::{Duration, Instant};

use testlab_schema::EnvironmentOperationKind;

use crate::compose::DockerComposeEnvironment;
use crate::compose_command::compose_owned;
use crate::compose_support::remaining;
use crate::compose_types::ComposeObservation;
use crate::observer_admin_target::AdminTarget;
use crate::user_scram_cli_observation;

impl DockerComposeEnvironment {
    pub(super) fn observe_user_scram_with_cli(
        &mut self,
        target: &AdminTarget,
        timeout: Duration,
    ) -> ComposeObservation {
        let mut observed = ComposeObservation::default();
        let AdminTarget::UserScramCredential(scram) = target else {
            observed.phase.fail(
                "environment_observation_failed",
                "non-user-SCRAM target reached user-SCRAM observer",
            );
            return observed;
        };
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            observed.phase.fail(
                "environment_observation_failed",
                "user-SCRAM observation deadline overflow",
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
                "no broker service for user-SCRAM query",
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
        args.extend(user_scram_cli_observation::selection(&scram.user));
        let operation = self.next_operation;
        let spec = compose_owned(
            EnvironmentOperationKind::BrokerObserve,
            &self.prefix,
            args,
            format!("user-scram-state-{operation:05}.txt"),
            format!("user-scram-state-{operation:05}.stderr.txt"),
        );
        let output = match self.execute(spec, remaining(deadline)) {
            Ok(output) => output,
            Err(error) => {
                observed.phase.fail(error.code, error.diagnostic);
                return observed;
            }
        };
        let result = user_scram_cli_observation::normalize(
            observation,
            &scram.operation_id,
            &scram.user,
            scram.mechanism,
            &output.stdout,
        );
        if !observed.phase.retain(output) {
            observed.phase.fail(
                "environment_observation_failed",
                "Kafka CLI user-SCRAM query failed",
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
