//! Batched topic-creation validation owns ordered resource shapes and shared identity.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

const MAX_BATCH_TOPICS: usize = 32;

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    let ScenarioAction::CreateTopicsBatch(action) = action else {
        return false;
    };
    validate_identity(
        &action.client_id,
        &action.operation_id,
        clients,
        operation_ids,
        problems,
    );
    if !(2..=MAX_BATCH_TOPICS).contains(&action.topics.len()) {
        problems.push(format!(
            "admin operation {} topics must contain 2 to {MAX_BATCH_TOPICS} entries",
            action.operation_id
        ));
    }
    validate_items(action, problems);
    validate_timeout(&action.operation_id, action.timeout_ms, problems);
    true
}

fn validate_items(action: &crate::CreateTopicsBatchAction, problems: &mut Vec<String>) {
    let mut topics = BTreeSet::new();
    for item in &action.topics {
        if !topics.insert(item.topic.as_str()) {
            problems.push(format!(
                "admin operation {} batch topics must be unique",
                action.operation_id
            ));
        }
        if item.topic.is_empty() || item.topic.len() > 249 {
            problems.push(format!(
                "admin operation {} has invalid batch topic",
                action.operation_id
            ));
        }
        if !(1..=10_000).contains(&item.partitions) {
            problems.push(format!(
                "admin operation {} batch topic {} partitions must be between 1 and 10000",
                action.operation_id, item.topic
            ));
        }
        if !(1..=100).contains(&item.replication_factor) {
            problems.push(format!(
                "admin operation {} batch topic {} replication_factor must be between 1 and 100",
                action.operation_id, item.topic
            ));
        }
        if item
            .expected_error_code
            .as_deref()
            .is_some_and(|code| code != crate::TOPIC_ALREADY_EXISTS_ERROR_CODE)
        {
            problems.push(format!(
                "admin operation {} batch duplicate {} must expect error code {}",
                action.operation_id,
                item.topic,
                crate::TOPIC_ALREADY_EXISTS_ERROR_CODE
            ));
        }
    }
}
