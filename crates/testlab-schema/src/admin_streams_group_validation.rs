//! Streams-group lifecycle scenarios keep fixture identities and offsets exact.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

const MAX_GROUP_BYTES: usize = 255;
const MAX_FIXTURE_RECORDS: i64 = 1024;

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    let ScenarioAction::ExerciseStreamsGroupAdminLifecycle(action) = action else {
        return false;
    };
    validate_identity(
        &action.client_id,
        &action.operation_id,
        clients,
        operation_ids,
        problems,
    );
    validate_timeout(&action.operation_id, action.timeout_ms, problems);
    validate_group(&action.operation_id, &action.primary_group_id, problems);
    validate_group(&action.operation_id, &action.secondary_group_id, problems);
    if action.primary_group_id == action.secondary_group_id {
        problems.push(format!(
            "admin operation {} requires distinct Streams group ids",
            action.operation_id
        ));
    }
    if action.input_topic != crate::STREAMS_DEMO_INPUT_TOPIC
        || action.output_topic != crate::STREAMS_DEMO_OUTPUT_TOPIC
    {
        problems.push(format!(
            "admin operation {} must use the pinned Kafka Streams demo topics",
            action.operation_id
        ));
    }
    if !(1..=MAX_FIXTURE_RECORDS).contains(&action.expected_initial_offset) {
        problems.push(format!(
            "admin operation {} expected_initial_offset must be between 1 and {MAX_FIXTURE_RECORDS}",
            action.operation_id
        ));
    }
    if action.altered_offset < 0
        || action.altered_offset > action.expected_initial_offset
        || action.altered_offset == action.expected_initial_offset
    {
        problems.push(format!(
            "admin operation {} altered_offset must be a different available nonnegative offset",
            action.operation_id
        ));
    }
    true
}

fn validate_group(operation_id: &OperationId, value: &str, problems: &mut Vec<String>) {
    if value.is_empty()
        || value.len() > MAX_GROUP_BYTES
        || value.chars().any(|character| {
            !character.is_ascii_alphanumeric() && !matches!(character, '-' | '_' | '.')
        })
    {
        problems.push(format!(
            "admin operation {operation_id} has an invalid Streams group id"
        ));
    }
}
