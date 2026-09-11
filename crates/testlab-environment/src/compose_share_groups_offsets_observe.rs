//! Plural Share offset reads retain one independent CLI query per caller-ordered group.

use std::time::{Duration, Instant};

use crate::compose::DockerComposeEnvironment;
use crate::compose_support::remaining;
use crate::compose_types::ComposeObservation;
use crate::observer_admin_target::{AdminTarget, ShareGroupOffsetTarget, ordinal};

impl DockerComposeEnvironment {
    pub(super) fn observe_share_groups_offsets_with_cli(
        &mut self,
        target: &AdminTarget,
        timeout: Duration,
    ) -> ComposeObservation {
        let mut observed = ComposeObservation::default();
        let AdminTarget::ShareGroupsOffsets(target) = target else {
            observed.phase.fail(
                "environment_observation_failed",
                "non-offset target reached plural Share-group offset observer",
            );
            return observed;
        };
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            observed.phase.fail(
                "environment_observation_failed",
                "Share-group offset observation deadline overflow",
            );
            return observed;
        };
        let first =
            match self.begin_admin_observation(&AdminTarget::ShareGroupsOffsets(target.clone())) {
                Ok(first) => first,
                Err(error) => {
                    observed
                        .phase
                        .fail("environment_observation_failed", error.to_string());
                    return observed;
                }
            };
        let mut flat_index = 0;
        for (group_index, group) in target.groups.iter().enumerate() {
            let selection = vec![
                "--describe".to_owned(),
                "--offsets".to_owned(),
                "--group".to_owned(),
                group.group_id.clone(),
            ];
            let output = match self.execute_share_group_cli(selection, "offsets", deadline) {
                Ok(output) => output,
                Err(error) => {
                    observed.phase.fail(error.code, error.diagnostic);
                    return observed;
                }
            };
            let succeeded = output.succeeded();
            let normalized = group
                .offsets
                .iter()
                .map(|offset| {
                    let observation = ordinal(first, flat_index)?;
                    flat_index += 1;
                    crate::share_group_cli_observation::normalize(
                        observation,
                        &AdminTarget::ShareGroupOffset(ShareGroupOffsetTarget {
                            operation_id: target.operation_id.clone(),
                            group_id: group.group_id.clone(),
                            topic: offset.topic.clone(),
                            partition: offset.partition,
                        }),
                        &output.stdout,
                    )
                })
                .collect::<Result<Vec<_>, _>>();
            observed.phase.retain(output);
            if !succeeded {
                observed.phase.fail(
                    "environment_observation_failed",
                    "Kafka CLI Share-group offset query failed",
                );
                return observed;
            }
            match normalized {
                Ok(states) => observed.state_observations.extend(states),
                Err(error) => {
                    observed
                        .phase
                        .fail("environment_observation_failed", error.to_string());
                    return observed;
                }
            }
            if remaining(deadline).is_zero() && group_index + 1 < target.groups.len() {
                observed.phase.fail(
                    "environment_observation_failed",
                    "plural Share-group offset observation deadline elapsed",
                );
                return observed;
            }
        }
        observed
    }
}
