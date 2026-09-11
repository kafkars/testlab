//! Plural topic-deletion validation owns bounded ordered expectations.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction, TopicSelection};

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    let ScenarioAction::DeleteTopics(action) = action else {
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
        if action.selection == TopicSelection::TopicId && topic.expected_error_code.is_some() {
            problems.push(format!(
                "admin operation {} topic-ID deletion topic {} must exist",
                action.operation_id, topic.topic
            ));
        }
    }
    validate_timeout(&action.operation_id, action.timeout_ms, problems);
    true
}
