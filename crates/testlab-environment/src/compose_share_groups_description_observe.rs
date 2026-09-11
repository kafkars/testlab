//! Plural Share descriptions retain one independent state query per caller-ordered group.

use std::time::{Duration, Instant};

use crate::compose::DockerComposeEnvironment;
use crate::compose_support::remaining;
use crate::compose_types::ComposeObservation;
use crate::observer_admin_target::{AdminTarget, ShareGroupTarget, ordinal};

impl DockerComposeEnvironment {
    pub(super) fn observe_share_group_descriptions_with_cli(
        &mut self,
        target: &AdminTarget,
        timeout: Duration,
    ) -> ComposeObservation {
        let mut observed = ComposeObservation::default();
        let AdminTarget::ShareGroupDescriptions(target) = target else {
            observed.phase.fail(
                "environment_observation_failed",
                "non-description target reached plural Share-group observer",
            );
            return observed;
        };
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            observed.phase.fail(
                "environment_observation_failed",
                "Share-group observation deadline overflow",
            );
            return observed;
        };
        let first = match self
            .begin_admin_observation(&AdminTarget::ShareGroupDescriptions(target.clone()))
        {
            Ok(first) => first,
            Err(error) => {
                observed
                    .phase
                    .fail("environment_observation_failed", error.to_string());
                return observed;
            }
        };
        for (index, group_id) in target.group_ids.iter().enumerate() {
            let observation = match ordinal(first, index) {
                Ok(observation) => observation,
                Err(error) => {
                    observed
                        .phase
                        .fail("environment_observation_failed", error.to_string());
                    return observed;
                }
            };
            let selection = vec![
                "--describe".to_owned(),
                "--state".to_owned(),
                "--group".to_owned(),
                group_id.clone(),
            ];
            let output = match self.execute_share_group_cli(selection, "state", deadline) {
                Ok(output) => output,
                Err(error) => {
                    observed.phase.fail(error.code, error.diagnostic);
                    return observed;
                }
            };
            let succeeded = output.succeeded();
            let state_target = AdminTarget::ShareGroup(ShareGroupTarget {
                operation_id: target.operation_id.clone(),
                group_id: group_id.clone(),
            });
            let normalized = crate::share_group_cli_observation::normalize(
                observation,
                &state_target,
                &output.stdout,
            );
            observed.phase.retain(output);
            if !succeeded {
                observed.phase.fail(
                    "environment_observation_failed",
                    "Kafka CLI Share-group state query failed",
                );
                return observed;
            }
            match normalized {
                Ok(state) => observed.state_observations.push(state),
                Err(error) => {
                    observed
                        .phase
                        .fail("environment_observation_failed", error.to_string());
                    return observed;
                }
            }
            if remaining(deadline).is_zero() && index + 1 < target.group_ids.len() {
                observed.phase.fail(
                    "environment_observation_failed",
                    "plural Share-group state observation deadline elapsed",
                );
                return observed;
            }
        }
        observed
    }
}
