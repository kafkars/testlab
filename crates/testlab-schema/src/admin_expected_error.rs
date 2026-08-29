//! Admin failure expectations remain scenario facts outside adapter commands.

use std::collections::BTreeMap;

use crate::{OperationId, ScenarioAction};

/// Returns the stable operation identity and exact expected public error.
pub fn expected_admin_error(action: &ScenarioAction) -> Option<(&OperationId, &str)> {
    match action {
        ScenarioAction::CreateTopic(action) => action
            .expected_error_code
            .as_deref()
            .map(|code| (&action.operation_id, code)),
        ScenarioAction::CreatePartitions(action) => action
            .expected_error_code
            .as_deref()
            .map(|code| (&action.operation_id, code)),
        ScenarioAction::DeleteTopic(action) => action
            .expected_error_code
            .as_deref()
            .map(|code| (&action.operation_id, code)),
        ScenarioAction::DescribeTopic(action) => action
            .expected_error_code
            .as_deref()
            .map(|code| (&action.operation_id, code)),
        ScenarioAction::ListOffsets(action) => action
            .expected_error_code
            .as_deref()
            .map(|code| (&action.operation_id, code)),
        _ => None,
    }
}

pub(crate) fn require_untracked_topic(
    operation_id: &OperationId,
    topic: &str,
    created_topics: &BTreeMap<String, (i32, i16)>,
    problems: &mut Vec<String>,
) {
    if created_topics.contains_key(topic) {
        problems.push(format!(
            "admin operation {operation_id} expects missing topic {topic} but a prior action created it"
        ));
    }
}
