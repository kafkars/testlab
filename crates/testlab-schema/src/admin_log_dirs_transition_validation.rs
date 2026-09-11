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
            ScenarioAction::DescribeLogDirs(action) => {
                let Some((partitions, replicas)) = topics.get(&action.topic) else {
                    problems.push(format!(
                        "admin operation {} requires a prior successful topic creation",
                        action.operation_id
                    ));
                    continue;
                };
                if action.partition >= *partitions {
                    problems.push(format!(
                        "admin operation {} selects partition {} outside the prior {}-partition topic",
                        action.operation_id, action.partition, partitions
                    ));
                }
                if usize::try_from(*replicas).ok() != Some(action.expected_replica_count) {
                    problems.push(format!(
                        "admin operation {} expected_replica_count must equal the prior topic replication factor",
                        action.operation_id
                    ));
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
#[path = "admin_log_dirs_transition_validation_test.rs"]
mod tests;
