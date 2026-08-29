//! Cluster and consumer-group admin validation owns bounded group-state intent.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_resource, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

const MAX_REQUIRED_GROUPS: usize = 32;

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    if crate::admin_group_plural_action_validation::validate(
        action,
        clients,
        operation_ids,
        problems,
    ) {
        return;
    }
    validate_singleton(action, clients, operation_ids, problems);
}

fn validate_singleton(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    if validate_single_offset(action, clients, operation_ids, problems) {
        return;
    }
    match action {
        ScenarioAction::DescribeCluster(action) => {
            validate_identity(
                &action.client_id,
                &action.operation_id,
                clients,
                operation_ids,
                problems,
            );
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        ScenarioAction::ListConsumerGroups(action) => {
            validate_identity(
                &action.client_id,
                &action.operation_id,
                clients,
                operation_ids,
                problems,
            );
            required_groups(&action.operation_id, &action.required_group_ids, problems);
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        ScenarioAction::DescribeConsumerGroup(action) => {
            group_common(
                &action.client_id,
                &action.operation_id,
                &action.group_id,
                clients,
                operation_ids,
                problems,
            );
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        ScenarioAction::DeleteConsumerGroup(action) => {
            group_common(
                &action.client_id,
                &action.operation_id,
                &action.group_id,
                clients,
                operation_ids,
                problems,
            );
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        _ => {}
    }
}

fn validate_single_offset(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    let (client_id, operation_id, group_id, topic, partition, offset, timeout_ms) = match action {
        ScenarioAction::ListConsumerGroupOffsets(action) => (
            &action.client_id,
            &action.operation_id,
            action.group_id.as_str(),
            action.topic.as_str(),
            action.partition,
            Some(("expected_offset", action.expected_offset)),
            action.timeout_ms,
        ),
        ScenarioAction::AlterConsumerGroupOffset(action) => (
            &action.client_id,
            &action.operation_id,
            action.group_id.as_str(),
            action.topic.as_str(),
            action.partition,
            Some(("offset", action.offset)),
            action.timeout_ms,
        ),
        ScenarioAction::DeleteConsumerGroupOffset(action) => (
            &action.client_id,
            &action.operation_id,
            action.group_id.as_str(),
            action.topic.as_str(),
            action.partition,
            None,
            action.timeout_ms,
        ),
        _ => return false,
    };
    offset_common(
        client_id,
        operation_id,
        group_id,
        topic,
        partition,
        clients,
        operation_ids,
        problems,
    );
    if let Some((field, value)) = offset
        && value < 0
    {
        problems.push(format!(
            "admin operation {operation_id} {field} must be nonnegative"
        ));
    }
    validate_timeout(operation_id, timeout_ms, problems);
    true
}

fn group_common(
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
    reason = "group-offset validation mirrors every explicit scenario resource field"
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
    group_common(
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

fn required_groups(operation_id: &OperationId, values: &[String], problems: &mut Vec<String>) {
    if values.is_empty() || values.len() > MAX_REQUIRED_GROUPS {
        problems.push(format!("admin operation {operation_id} required_group_ids must contain 1 to {MAX_REQUIRED_GROUPS} entries"));
    }
    let mut unique = BTreeSet::new();
    for group_id in values {
        if group_id.is_empty() || group_id.len() > 255 || !unique.insert(group_id) {
            problems.push(format!(
                "admin operation {operation_id} required_group_ids must contain unique valid groups"
            ));
        }
    }
}
