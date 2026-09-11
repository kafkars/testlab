//! Log-directory snapshots use Kafka's pinned administration CLI.

use std::thread;
use std::time::{Duration, Instant};

use testlab_schema::{
    BrokerStateObservation, EnvironmentOperationKind, ReplicaLogDirAssignmentSpec,
};

use crate::compose::DockerComposeEnvironment;
use crate::compose_command::compose_owned;
use crate::compose_support::remaining;
use crate::compose_types::ComposeObservation;
use crate::observer_admin_log_dirs_target::LogDirsTarget;
use crate::observer_admin_target::AdminTarget;

const POLL_SLICE: Duration = Duration::from_millis(50);

impl DockerComposeEnvironment {
    pub(super) fn observe_log_dirs_with_cli(
        &mut self,
        target: &AdminTarget,
        timeout: Duration,
    ) -> ComposeObservation {
        let mut observed = ComposeObservation::default();
        let observation_target = target;
        let (target, assignments) = match target {
            AdminTarget::LogDirs(target) | AdminTarget::ReplicaLogDirs(target) => {
                (target.clone(), None)
            }
            AdminTarget::ReplicaLogDirsAlteration(target) => (
                LogDirsTarget {
                    operation_id: target.operation_id.clone(),
                    topic: target.topic.clone(),
                    partition: target.partition,
                },
                Some(target.assignments.as_slice()),
            ),
            _ => {
                observed.phase.fail(
                    "environment_observation_failed",
                    "non-log-directory target reached log-directory observer",
                );
                return observed;
            }
        };
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            observed.phase.fail(
                "environment_observation_failed",
                "log-directory observation deadline overflow",
            );
            return observed;
        };
        let observation = match self.begin_admin_observation(observation_target) {
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
                "no broker service for log-directory query",
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
                    service.clone(),
                    "/opt/kafka/bin/kafka-log-dirs.sh".to_owned(),
                    "--bootstrap-server".to_owned(),
                    format!("localhost:{}", self.client_port),
                    "--describe".to_owned(),
                    "--topic-list".to_owned(),
                    target.topic.clone(),
                ],
                format!("log-dirs-{operation:05}.json"),
                format!("log-dirs-{operation:05}.stderr.txt"),
            );
            let output = match self.execute(spec, remaining(deadline)) {
                Ok(value) => value,
                Err(error) => {
                    observed.phase.fail(error.code, error.diagnostic);
                    return observed;
                }
            };
            let stdout = output.stdout.clone();
            if !observed.phase.retain(output) {
                observed.phase.fail(
                    "environment_observation_failed",
                    "Kafka CLI log-directory query failed",
                );
                return observed;
            }
            let state =
                match crate::log_dirs_cli_observation::normalize(observation, &target, &stdout) {
                    Ok(state) => state,
                    Err(error) => {
                        observed
                            .phase
                            .fail("environment_observation_failed", error.to_string());
                        return observed;
                    }
                };
            if assignments.is_none_or(|assignments| settled(&state, assignments)) {
                observed.state_observations.push(state);
                return observed;
            }
            let wait = remaining(deadline);
            if wait.is_zero() {
                observed.phase.fail(
                    "environment_observation_failed",
                    "replica log-directory alteration did not converge before its deadline",
                );
                return observed;
            }
            thread::sleep(POLL_SLICE.min(wait));
        }
    }
}

fn settled(state: &BrokerStateObservation, assignments: &[ReplicaLogDirAssignmentSpec]) -> bool {
    let BrokerStateObservation::LogDirs(state) = state else {
        return false;
    };
    assignments.iter().all(|assignment| {
        let placements = state
            .brokers
            .iter()
            .filter(|broker| broker.broker_id == assignment.broker_id)
            .flat_map(|broker| &broker.log_dirs)
            .flat_map(|directory| {
                directory
                    .replicas
                    .iter()
                    .filter(|replica| {
                        replica.topic == assignment.topic
                            && replica.partition == assignment.partition
                    })
                    .map(|replica| (directory.path.as_str(), replica.is_future))
            })
            .collect::<Vec<_>>();
        placements.len() == 1 && placements[0] == (assignment.target_path.as_str(), false)
    })
}
