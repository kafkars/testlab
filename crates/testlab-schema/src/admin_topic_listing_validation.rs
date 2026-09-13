//! Topic-list expectations are bounded, unique, and coherent with the public option.

use std::collections::BTreeSet;

use crate::ListTopicsAction;

const MAX_EXPECTED_TOPICS: usize = 32;

pub(super) fn validate(action: &ListTopicsAction, problems: &mut Vec<String>) {
    if action.expected_topics.is_empty() || action.expected_topics.len() > MAX_EXPECTED_TOPICS {
        problems.push(format!(
            "admin operation {} expected_topics must contain 1 to {MAX_EXPECTED_TOPICS} entries",
            action.operation_id
        ));
    }
    let mut unique = BTreeSet::new();
    if action.expected_topics.iter().any(|value| {
        value.topic.is_empty() || value.topic.len() > 249 || !unique.insert(&value.topic)
    }) {
        problems.push(format!(
            "admin operation {} expected_topics must contain unique valid topics",
            action.operation_id
        ));
    }
    if action
        .expected_topics
        .iter()
        .any(|value| value.included != (!value.internal || action.include_internal))
    {
        problems.push(format!(
            "admin operation {} expected_topics inclusion must match internal classification and include_internal",
            action.operation_id
        ));
    }
}
