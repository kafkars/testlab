//! Plural topic-description validation owns bounded ordered expectations.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction, TopicSelection};

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    let ScenarioAction::DescribeTopics(action) = action else {
        return false;
    };
    validate_identity(
        &action.client_id,
        &action.operation_id,
        clients,
        operation_ids,
        problems,
    );
    if !(2..=32).contains(&action.topics.len()) {
        problems.push(format!(
            "admin operation {} topics must contain between 2 and 32 entries",
            action.operation_id
        ));
    }
    let mut topics = BTreeSet::new();
    for topic in &action.topics {
        if topic.topic.is_empty() || topic.topic.len() > 249 || !topics.insert(&topic.topic) {
            problems.push(format!(
                "admin operation {} topics must contain unique valid names",
                action.operation_id
            ));
        }
        if topic.expected_partitions.is_some() == topic.expected_error_code.is_some() {
            problems.push(format!(
                "admin operation {} topic {} must declare exactly one public outcome",
                action.operation_id, topic.topic
            ));
        }
        if action.selection == TopicSelection::TopicId && topic.expected_error_code.is_some() {
            problems.push(format!(
                "admin operation {} topic-ID lookup topic {} must exist",
                action.operation_id, topic.topic
            ));
        }
        if let Some(partitions) = &topic.expected_partitions {
            validate_partitions(&action.operation_id, partitions, problems);
        }
        if topic
            .expected_error_code
            .as_deref()
            .is_some_and(|code| code != crate::UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE)
        {
            problems.push(format!(
                "admin operation {} topic {} must expect error code {}",
                action.operation_id,
                topic.topic,
                crate::UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE
            ));
        }
    }
    validate_timeout(&action.operation_id, action.timeout_ms, problems);
    true
}

fn validate_partitions(operation_id: &OperationId, partitions: &[i32], problems: &mut Vec<String>) {
    if partitions.is_empty() || partitions.len() > 10_000 {
        problems.push(format!(
            "admin operation {operation_id} expected_partitions must contain 1 to 10000 entries"
        ));
    }
    if partitions.iter().any(|partition| *partition < 0)
        || partitions.windows(2).any(|pair| pair[0] >= pair[1])
    {
        problems.push(format!(
            "admin operation {operation_id} expected_partitions must be sorted unique nonnegative indices"
        ));
    }
}
