//! Final Streams-group absence is polled through Kafka's pinned CLI.

use std::thread;
use std::time::{Duration, Instant};

use testlab_schema::{BrokerStateObservation, EnvironmentOperationKind};

use crate::compose::DockerComposeEnvironment;
use crate::compose_command::compose_owned;
use crate::compose_support::remaining;
use crate::compose_types::ComposeObservation;
use crate::observer_admin_target::AdminTarget;

const POLL_SLICE: Duration = Duration::from_millis(50);
const SCRIPT: &str = r#"set -euo pipefail
groups=$(/opt/kafka/bin/kafka-streams-groups.sh --bootstrap-server broker-1:19092 --list 2>&1)
for group in "$@"; do
  if awk -v group="$group" '$0 == group { found = 1 } END { exit found ? 0 : 1 }' <<<"$groups"; then
    printf 'streams-group-present:%s\n' "$group"
  else
    printf 'streams-group-absent:%s\n' "$group"
  fi
done
"#;

impl DockerComposeEnvironment {
    pub(super) fn observe_streams_groups_with_cli(
        &mut self,
        target: &AdminTarget,
        timeout: Duration,
    ) -> ComposeObservation {
        let mut observed = ComposeObservation::default();
        let AdminTarget::StreamsGroupsLifecycle(target) = target else {
            observed.phase.fail(
                "environment_observation_failed",
                "non-Streams target reached Streams-group observer",
            );
            return observed;
        };
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            observed.phase.fail(
                "environment_observation_failed",
                "Streams-group observation deadline overflow",
            );
            return observed;
        };
        let observation = match self
            .begin_admin_observation(&AdminTarget::StreamsGroupsLifecycle(target.clone()))
        {
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
                "no broker service for Streams-group query",
            );
            return observed;
        };
        loop {
            let operation = self.next_operation;
            let mut args = vec![
                "exec".to_owned(),
                "--no-TTY".to_owned(),
                service.clone(),
                "/bin/bash".to_owned(),
                "-euc".to_owned(),
                SCRIPT.to_owned(),
                "testlab-streams-group-observer".to_owned(),
            ];
            args.extend(target.group_ids.iter().cloned());
            let spec = compose_owned(
                EnvironmentOperationKind::BrokerObserve,
                &self.prefix,
                args,
                format!("streams-group-state-{operation:05}.txt"),
                format!("streams-group-state-{operation:05}.stderr.txt"),
            );
            let output = match self.execute(spec, remaining(deadline)) {
                Ok(value) => value,
                Err(error) => {
                    observed.phase.fail(error.code, error.diagnostic);
                    return observed;
                }
            };
            let state = crate::streams_group_cli_observation::normalize(
                observation,
                &target.operation_id,
                &target.group_ids,
                &output.stdout,
            );
            if !observed.phase.retain(output) {
                observed.phase.fail(
                    "environment_observation_failed",
                    "Kafka Streams CLI query failed",
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
            if state.all_absent {
                observed
                    .state_observations
                    .push(BrokerStateObservation::StreamsGroups(state));
                return observed;
            }
            let wait = remaining(deadline);
            if wait.is_zero() {
                observed.phase.fail(
                    "environment_observation_failed",
                    "Streams groups remained visible past the deletion deadline",
                );
                return observed;
            }
            thread::sleep(POLL_SLICE.min(wait));
        }
    }
}

#[cfg(test)]
#[path = "compose_streams_group_observe_test.rs"]
mod tests;
