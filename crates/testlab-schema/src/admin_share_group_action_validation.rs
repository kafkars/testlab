//! Share-group Admin validation owns bounded state and partition intent.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_resource, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    match action {
        ScenarioAction::DescribeShareGroup(action) => {
            common(
                &action.client_id,
                &action.operation_id,
                &action.group_id,
                clients,
                operation_ids,
                problems,
            );
            if action.expected_state != "Stable" {
                problems.push(format!(
                    "admin operation {} expected_state must be Stable",
                    action.operation_id
                ));
            }
            if action.expected_member_count == 0 || action.expected_member_count > 32 {
                problems.push(format!(
                    "admin operation {} expected_member_count must be between 1 and 32",
                    action.operation_id
                ));
            }
            if action.expected_topic.is_empty() || action.expected_topic.len() > 249 {
                problems.push(format!(
                    "admin operation {} has invalid expected_topic",
                    action.operation_id
                ));
            }
            if action.expected_partition < 0 {
                problems.push(format!(
                    "admin operation {} expected_partition must be nonnegative",
                    action.operation_id
                ));
            }
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        ScenarioAction::ListShareGroupOffsets(action) => {
            offset_common(
                &action.client_id,
                &action.operation_id,
                &action.group_id,
                &action.topic,
                action.partition,
                clients,
                operation_ids,
                problems,
            );
            validate_nonnegative(
                &action.operation_id,
                [
                    ("expected_start_offset", action.expected_start_offset),
                    ("expected_lag", action.expected_lag),
                ],
                problems,
            );
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        ScenarioAction::AlterShareGroupOffsets(action) => {
            offset_common(
                &action.client_id,
                &action.operation_id,
                &action.group_id,
                &action.topic,
                action.partition,
                clients,
                operation_ids,
                problems,
            );
            validate_nonnegative(
                &action.operation_id,
                [
                    ("start_offset", action.start_offset),
                    ("expected_lag", action.expected_lag),
                ],
                problems,
            );
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        _ => return false,
    }
    true
}

fn common(
    client_id: &ClientId,
    operation_id: &OperationId,
    group_id: &str,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    validate_resource(
        client_id,
        operation_id,
        group_id,
        "group_id",
        255,
        clients,
        operation_ids,
        problems,
    );
}

#[allow(
    clippy::too_many_arguments,
    reason = "Share-group offset validation keeps every resource field explicit"
)]
fn offset_common(
    client_id: &ClientId,
    operation_id: &OperationId,
    group_id: &str,
    topic: &str,
    partition: i32,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    common(
        client_id,
        operation_id,
        group_id,
        clients,
        operation_ids,
        problems,
    );
    if topic.is_empty() || topic.len() > 249 {
        problems.push(format!("admin operation {operation_id} has invalid topic"));
    }
    if partition < 0 {
        problems.push(format!(
            "admin operation {operation_id} partition must be nonnegative"
        ));
    }
}

fn validate_nonnegative(
    operation_id: &OperationId,
    fields: [(&str, i64); 2],
    problems: &mut Vec<String>,
) {
    for (field, value) in fields {
        if value < 0 {
            problems.push(format!(
                "admin operation {operation_id} {field} must be nonnegative"
            ));
        }
    }
}
