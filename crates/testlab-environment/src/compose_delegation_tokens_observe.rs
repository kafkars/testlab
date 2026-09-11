//! Delegation-token absence is polled through a secret-free pinned CLI projection.

use std::thread;
use std::time::{Duration, Instant};

use testlab_schema::{BrokerStateObservation, EnvironmentOperationKind, SASL_PASSWORD_ENVIRONMENT};

use crate::compose::DockerComposeEnvironment;
use crate::compose_command::compose_owned;
use crate::compose_support::remaining;
use crate::compose_types::ComposeObservation;
use crate::observer_admin_target::AdminTarget;

const POLL_SLICE: Duration = Duration::from_millis(50);
const SCRIPT: &str = r#"set -o pipefail
properties=$(mktemp)
trap 'rm -f "$properties"' EXIT
printf '%s\n' \
  'security.protocol=SASL_PLAINTEXT' \
  'sasl.mechanism=PLAIN' \
  "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafkars\" password=\"$TESTLAB_KAFKA_SASL_PASSWORD\";" \
  > "$properties"
/opt/kafka/bin/kafka-delegation-tokens.sh \
  --bootstrap-server broker-1:39092 \
  --describe \
  --command-config "$properties" \
  --owner-principal "$1" 2>&1 |
awk '$1 == "Total" && $2 == "number" && $3 == "of" && $4 == "tokens" && $5 == ":" { print "token-count:" $6; found = 1 } END { if (!found) exit 42 }'
"#;

impl DockerComposeEnvironment {
    pub(super) fn observe_delegation_tokens_with_cli(
        &mut self,
        target: &AdminTarget,
        timeout: Duration,
    ) -> ComposeObservation {
        let mut observed = ComposeObservation::default();
        let AdminTarget::DelegationTokens(target) = target else {
            observed.phase.fail(
                "environment_observation_failed",
                "non-delegation-token target reached delegation-token observer",
            );
            return observed;
        };
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            observed.phase.fail(
                "environment_observation_failed",
                "delegation-token observation deadline overflow",
            );
            return observed;
        };
        let observation =
            match self.begin_admin_observation(&AdminTarget::DelegationTokens(target.clone())) {
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
                "no broker service for delegation-token query",
            );
            return observed;
        };
        loop {
            let operation = self.next_operation;
            let spec = compose_owned(
                EnvironmentOperationKind::BrokerObserve,
                &self.prefix,
                vec![
                    "exec".to_owned(),
                    "--no-TTY".to_owned(),
                    "--env".to_owned(),
                    SASL_PASSWORD_ENVIRONMENT.to_owned(),
                    service.clone(),
                    "/bin/bash".to_owned(),
                    "-euc".to_owned(),
                    SCRIPT.to_owned(),
                    "testlab-delegation-token-observer".to_owned(),
                    target.owner.kafka_name(),
                ],
                format!("delegation-token-state-{operation:05}.txt"),
                format!("delegation-token-state-{operation:05}.stderr.txt"),
            );
            let output = match self.execute(spec, remaining(deadline)) {
                Ok(value) => value,
                Err(error) => {
                    observed.phase.fail(error.code, error.diagnostic);
                    return observed;
                }
            };
            let state = crate::delegation_token_cli_observation::normalize(
                observation,
                &target.operation_id,
                &target.owner,
                &output.stdout,
            );
            if !observed.phase.retain(output) {
                observed.phase.fail(
                    "environment_observation_failed",
                    "Kafka CLI delegation-token query failed",
                );
                return observed;
            }
            let state = match state {
                Ok(state) => state,
                Err(error) => {
                    observed
                        .phase
                        .fail("environment_observation_failed", error.to_string());
                    return observed;
                }
            };
            if state.token_count == 0 {
                observed
                    .state_observations
                    .push(BrokerStateObservation::DelegationTokens(state));
                return observed;
            }
            let wait = remaining(deadline);
            if wait.is_zero() {
                observed.phase.fail(
                    "environment_observation_failed",
                    "delegation token remained visible past its expiration deadline",
                );
                return observed;
            }
            thread::sleep(POLL_SLICE.min(wait));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SCRIPT;

    #[test]
    fn cli_projection_retains_only_the_token_count() {
        assert!(SCRIPT.contains("$TESTLAB_KAFKA_SASL_PASSWORD"));
        assert!(!SCRIPT.contains("kafkars-testlab-password"));
        assert!(SCRIPT.contains("print \"token-count:\" $6"));
        assert!(SCRIPT.contains("2>&1 |"));
    }
}
