//! Plural group-admin validation owns bounded ordered resource selections.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_resource, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

const MAX_ITEMS: usize = 32;

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    match action {
        ScenarioAction::ListConsumerGroupOffsetsBatch(action) => {
            group_identity(
                &action.client_id,
                &action.operation_id,
                &action.group_id,
                clients,
                operation_ids,
                problems,
            );
            expectations(&action.operation_id, &action.partitions, problems);
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        ScenarioAction::ListConsumerGroupsOffsets(action) => {
            validate_identity(
                &action.client_id,
                &action.operation_id,
                clients,
                operation_ids,
                problems,
            );
            group_expectations(&action.operation_id, &action.groups, problems);
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        ScenarioAction::AlterConsumerGroupOffsets(action) => {
            group_identity(
                &action.client_id,
                &action.operation_id,
                &action.group_id,
                clients,
                operation_ids,
                problems,
            );
            alterations(&action.operation_id, &action.offsets, problems);
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        ScenarioAction::DeleteConsumerGroupOffsets(action) => {
            group_identity(
                &action.client_id,
                &action.operation_id,
                &action.group_id,
                clients,
                operation_ids,
                problems,
            );
            selections(&action.operation_id, &action.partitions, problems);
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        ScenarioAction::DescribeClassicGroups(action) => {
            validate_identity(
                &action.client_id,
                &action.operation_id,
                clients,
                operation_ids,
                problems,
            );
            classic_groups(&action.operation_id, &action.groups, problems);
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        _ => return false,
    }
    true
}

fn group_identity(
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

fn expectations(
    operation_id: &OperationId,
    values: &[crate::ConsumerGroupOffsetExpectation],
    problems: &mut Vec<String>,
) {
    item_count(operation_id, "partitions", values.len(), problems);
    let mut keys = BTreeSet::new();
    for value in values {
        selection(
            operation_id,
            &value.topic,
            value.partition,
            &mut keys,
            problems,
        );
        if value.expected_offset < 0 {
            problems.push(format!(
                "admin operation {operation_id} expected_offset must be nonnegative"
            ));
        }
    }
}

fn group_expectations(
    operation_id: &OperationId,
    groups: &[crate::ConsumerGroupOffsetsExpectation],
    problems: &mut Vec<String>,
) {
    item_count(operation_id, "groups", groups.len(), problems);
    let mut group_ids = BTreeSet::new();
    for group in groups {
        unique_group(operation_id, &group.group_id, &mut group_ids, problems);
        expectations(operation_id, &group.partitions, problems);
    }
}

fn alterations(
    operation_id: &OperationId,
    values: &[crate::ConsumerGroupOffsetAlteration],
    problems: &mut Vec<String>,
) {
    item_count(operation_id, "offsets", values.len(), problems);
    let mut keys = BTreeSet::new();
    for value in values {
        selection(
            operation_id,
            &value.topic,
            value.partition,
            &mut keys,
            problems,
        );
        if value.offset < 0 {
            problems.push(format!(
                "admin operation {operation_id} offset must be nonnegative"
            ));
        }
    }
}

fn selections(
    operation_id: &OperationId,
    values: &[crate::ConsumerGroupOffsetSelection],
    problems: &mut Vec<String>,
) {
    item_count(operation_id, "partitions", values.len(), problems);
    let mut keys = BTreeSet::new();
    for value in values {
        selection(
            operation_id,
            &value.topic,
            value.partition,
            &mut keys,
            problems,
        );
    }
}

fn classic_groups(
    operation_id: &OperationId,
    groups: &[crate::ClassicGroupExpectation],
    problems: &mut Vec<String>,
) {
    item_count(operation_id, "groups", groups.len(), problems);
    let mut group_ids = BTreeSet::new();
    for group in groups {
        unique_group(operation_id, &group.group_id, &mut group_ids, problems);
    }
}

fn selection(
    operation_id: &OperationId,
    topic: &str,
    partition: i32,
    keys: &mut BTreeSet<(String, i32)>,
    problems: &mut Vec<String>,
) {
    if topic.is_empty() || topic.len() > 249 {
        problems.push(format!("admin operation {operation_id} has invalid topic"));
    }
    if partition < 0 {
        problems.push(format!(
            "admin operation {operation_id} partition must be nonnegative"
        ));
    }
    if !keys.insert((topic.to_owned(), partition)) {
        problems.push(format!(
            "admin operation {operation_id} contains duplicate topic-partition {topic}:{partition}"
        ));
    }
}

fn unique_group(
    operation_id: &OperationId,
    group_id: &str,
    group_ids: &mut BTreeSet<String>,
    problems: &mut Vec<String>,
) {
    if group_id.is_empty() || group_id.len() > 255 || !group_ids.insert(group_id.to_owned()) {
        problems.push(format!(
            "admin operation {operation_id} groups must contain unique valid group ids"
        ));
    }
}

fn item_count(operation_id: &OperationId, field: &str, count: usize, problems: &mut Vec<String>) {
    if !(1..=MAX_ITEMS).contains(&count) {
        problems.push(format!(
            "admin operation {operation_id} {field} must contain 1 to {MAX_ITEMS} entries"
        ));
    }
}
