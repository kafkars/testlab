//! Log-directory discovery requires a stable scenario-owned replica fixture.

use std::collections::BTreeMap;

use crate::{Scenario, ScenarioAction};

pub(crate) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut topics = BTreeMap::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::CreateTopic(action)
                if action.expected_error_code.is_none() && !action.validate_only =>
            {
                topics.insert(
                    action.topic.clone(),
                    (action.partitions, action.replication_factor),
                );
            }
            ScenarioAction::DescribeLogDirs(action) => validate_target(
                &action.operation_id,
                &action.topic,
                action.partition,
                action.expected_replica_count,
                &topics,
                problems,
            ),
            ScenarioAction::DescribeReplicaLogDirs(action) => validate_target(
                &action.operation_id,
                &action.topic,
                action.partition,
                action.expected_replica_count,
                &topics,
                problems,
            ),
            _ => {}
        }
    }
}

fn validate_target(
    operation_id: &crate::OperationId,
    topic: &str,
    partition: i32,
    expected_replica_count: usize,
    topics: &BTreeMap<String, (i32, i16)>,
    problems: &mut Vec<String>,
) {
    let Some((partitions, replicas)) = topics.get(topic) else {
        problems.push(format!(
            "admin operation {operation_id} requires a prior successful topic creation"
        ));
        return;
    };
    if partition >= *partitions {
        problems.push(format!(
            "admin operation {operation_id} selects partition {partition} outside the prior {partitions}-partition topic"
        ));
    }
    if usize::try_from(*replicas).ok() != Some(expected_replica_count) {
        problems.push(format!(
            "admin operation {operation_id} expected_replica_count must equal the prior topic replication factor"
        ));
    }
}

#[cfg(test)]
#[path = "admin_log_dirs_transition_validation_test.rs"]
mod tests;
