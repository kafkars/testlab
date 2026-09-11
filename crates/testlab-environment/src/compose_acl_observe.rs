//! ACL snapshots use the pinned Kafka CLI and retain one raw query per exact binding.

use std::time::{Duration, Instant};

use testlab_schema::EnvironmentOperationKind;

use crate::acl_cli_observation;
use crate::compose::DockerComposeEnvironment;
use crate::compose_command::compose_owned;
use crate::compose_support::remaining;
use crate::compose_types::ComposeObservation;
use crate::observer_admin_target::{AdminTarget, ordinal};

impl DockerComposeEnvironment {
    pub(super) fn observe_acls_with_cli(
        &mut self,
        target: &AdminTarget,
        timeout: Duration,
    ) -> ComposeObservation {
        let mut observed = ComposeObservation::default();
        let AdminTarget::Acls(acls) = target else {
            observed.phase.fail(
                "environment_observation_failed",
                "non-ACL target reached ACL observer",
            );
            return observed;
        };
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            observed.phase.fail(
                "environment_observation_failed",
                "ACL observation deadline overflow",
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
                "no broker service for ACL query",
            );
            return observed;
        };
        for (index, binding) in acls.bindings.iter().enumerate() {
            let mut args = vec![
                "exec".to_owned(),
                "--no-TTY".to_owned(),
                service.clone(),
                "/opt/kafka/bin/kafka-acls.sh".to_owned(),
                "--bootstrap-server".to_owned(),
                format!("localhost:{}", self.client_port),
                "--list".to_owned(),
            ];
            args.extend(acl_cli_observation::selection(binding));
            let operation = self.next_operation;
            let spec = compose_owned(
                EnvironmentOperationKind::BrokerObserve,
                &self.prefix,
                args,
                format!("acl-state-{operation:05}-{index:02}.txt"),
                format!("acl-state-{operation:05}-{index:02}.stderr.txt"),
            );
            let output = match self.execute(spec, remaining(deadline)) {
                Ok(output) => output,
                Err(error) => {
                    observed.phase.fail(error.code, error.diagnostic);
                    return observed;
                }
            };
            let result = ordinal(first, index).and_then(|observation| {
                acl_cli_observation::normalize(
                    observation,
                    &acls.operation_id,
                    binding,
                    &output.stdout,
                )
            });
            if !observed.phase.retain(output) {
                observed.phase.fail(
                    "environment_observation_failed",
                    "Kafka CLI ACL query failed",
                );
                return observed;
            }
            match result {
                Ok(state) => observed.state_observations.push(state),
                Err(error) => {
                    observed
                        .phase
                        .fail("environment_observation_failed", error.to_string());
                    return observed;
                }
            }
        }
        observed
    }
}
