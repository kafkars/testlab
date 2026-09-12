//! Share-group Admin validation owns bounded state and partition intent.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_resource, validate_timeout};
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
            description_expectation(
                &action.operation_id,
                &action.expected_state,
                action.expected_member_count,
                action.expected_rack_id.as_deref(),
                &action.expected_topic,
                action.expected_partition,
                problems,
            );
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        ScenarioAction::DescribeShareGroups(action) => {
            validate_identity(
                &action.client_id,
                &action.operation_id,
                clients,
                operation_ids,
                problems,
            );
            if !(2..=32).contains(&action.groups.len()) {
                problems.push(format!(
                    "admin operation {} groups must contain between 2 and 32 entries",
                    action.operation_id
                ));
            }
            let mut groups = BTreeSet::new();
            for group in &action.groups {
                if group.group_id.is_empty()
                    || group.group_id.len() > 255
                    || !groups.insert(&group.group_id)
                {
                    problems.push(format!(
                        "admin operation {} groups must contain unique valid group ids",
                        action.operation_id
                    ));
                }
                description_expectation(
                    &action.operation_id,
                    &group.expected_state,
                    group.expected_member_count,
                    group.expected_rack_id.as_deref(),
                    &group.expected_topic,
                    group.expected_partition,
                    problems,
                );
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
        ScenarioAction::DeleteShareGroupOffsets(action) => {
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
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        ScenarioAction::DeleteShareGroups(action) => {
            validate_identity(
                &action.client_id,
                &action.operation_id,
                clients,
                operation_ids,
                problems,
            );
            if !(2..=32).contains(&action.group_ids.len()) {
                problems.push(format!(
                    "admin operation {} group_ids must contain between 2 and 32 groups",
                    action.operation_id
                ));
            }
            let mut groups = BTreeSet::new();
            for group_id in &action.group_ids {
                if group_id.is_empty() || group_id.len() > 255 {
                    problems.push(format!(
                        "admin operation {} has invalid group_id",
                        action.operation_id
                    ));
                }
                if !groups.insert(group_id) {
                    problems.push(format!(
                        "admin operation {} contains duplicate group_id {}",
                        action.operation_id, group_id
                    ));
                }
            }
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

fn description_expectation(
    operation_id: &OperationId,
    state: &str,
    member_count: u32,
    rack_id: Option<&str>,
    topic: &str,
    partition: i32,
    problems: &mut Vec<String>,
) {
    if state != "Stable" {
        problems.push(format!(
            "admin operation {operation_id} expected_state must be Stable"
        ));
    }
    if member_count == 0 || member_count > 32 {
        problems.push(format!(
            "admin operation {operation_id} expected_member_count must be between 1 and 32"
        ));
    }
    if rack_id.is_some_and(|rack_id| rack_id.is_empty() || rack_id.len() > 249) {
        problems.push(format!(
            "admin operation {operation_id} has invalid expected_rack_id"
        ));
    }
    if topic.is_empty() || topic.len() > 249 {
        problems.push(format!(
            "admin operation {operation_id} has invalid expected_topic"
        ));
    }
    if partition < 0 {
        problems.push(format!(
            "admin operation {operation_id} expected_partition must be nonnegative"
        ));
    }
}
