//! Reassignment fixtures require prior successful topic ownership and valid partitions.

use std::collections::BTreeMap;

use crate::{Scenario, ScenarioAction};

type TopicDefinition = (i32, i16);

pub(crate) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut topics = BTreeMap::<String, TopicDefinition>::new();
    let mut altered = Vec::<(String, i32)>::new();
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
            ScenarioAction::CreateTopicsBatch(action) => {
                for topic in &action.topics {
                    if topic.expected_error_code.is_none() {
                        topics.insert(
                            topic.topic.clone(),
                            (topic.partitions, topic.replication_factor),
                        );
                    }
                }
            }
            ScenarioAction::CreatePartitions(action)
                if action.expected_error_code.is_none() && !action.validate_only =>
            {
                if let Some((partitions, _)) = topics.get_mut(&action.topic) {
                    *partitions = action.total_count;
                }
            }
            ScenarioAction::AlterPartitionReassignments(action) => {
                for change in &action.changes {
                    validate_partition(
                        &action.operation_id,
                        &change.topic,
                        change.partition,
                        &topics,
                        problems,
                    );
                    if topics.get(&change.topic).is_some_and(|(_, replication)| {
                        usize::try_from(*replication).ok() != Some(change.replicas.len())
                    }) && !action.allow_replication_factor_change
                    {
                        problems.push(format!(
                            "admin operation {} changes replication factor without permission",
                            action.operation_id
                        ));
                    }
                    altered.push((change.topic.clone(), change.partition));
                }
            }
            ScenarioAction::ListPartitionReassignments(action) => {
                if altered.is_empty() {
                    problems.push(format!(
                        "admin operation {} requires a prior successful reassignment mutation",
                        action.operation_id
                    ));
                }
                if let Some(targets) = &action.targets {
                    for target in targets {
                        validate_partition(
                            &action.operation_id,
                            &target.topic,
                            target.partition,
                            &topics,
                            problems,
                        );
                        if !altered.contains(&(target.topic.clone(), target.partition)) {
                            problems.push(format!(
                                "admin operation {} selects a partition not previously reassigned",
                                action.operation_id
                            ));
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

fn validate_partition(
    operation_id: &crate::OperationId,
    topic: &str,
    partition: i32,
    topics: &BTreeMap<String, TopicDefinition>,
    problems: &mut Vec<String>,
) {
    if !topics
        .get(topic)
        .is_some_and(|(partitions, _)| partition >= 0 && partition < *partitions)
    {
        problems.push(format!(
            "admin operation {operation_id} requires prior successful creation of partition {topic}-{partition}"
        ));
    }
}
