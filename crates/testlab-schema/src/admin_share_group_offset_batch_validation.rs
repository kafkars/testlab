//! Plural Share-group offset validation owns bounded ordered selections.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    let ScenarioAction::ListShareGroupsOffsets(action) = action else {
        return false;
    };
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
        validate_partitions(action, group, problems);
    }
    validate_timeout(&action.operation_id, action.timeout_ms, problems);
    true
}

fn validate_partitions(
    action: &crate::ListShareGroupsOffsetsAction,
    group: &crate::ShareGroupOffsetsExpectation,
    problems: &mut Vec<String>,
) {
    if !(1..=32).contains(&group.partitions.len()) {
        problems.push(format!(
            "admin operation {} partitions must contain between 1 and 32 entries",
            action.operation_id
        ));
    }
    let mut partitions = BTreeSet::new();
    for partition in &group.partitions {
        if partition.topic.is_empty() || partition.topic.len() > 249 {
            problems.push(format!(
                "admin operation {} has invalid topic",
                action.operation_id
            ));
        }
        if partition.partition < 0 {
            problems.push(format!(
                "admin operation {} partition must be nonnegative",
                action.operation_id
            ));
        }
        if !partitions.insert((&partition.topic, partition.partition)) {
            problems.push(format!(
                "admin operation {} contains duplicate topic-partition {}:{}",
                action.operation_id, partition.topic, partition.partition
            ));
        }
        for (field, value) in [
            ("expected_start_offset", partition.expected_start_offset),
            ("expected_lag", partition.expected_lag),
        ] {
            if value < 0 {
                problems.push(format!(
                    "admin operation {} {field} must be nonnegative",
                    action.operation_id
                ));
            }
        }
    }
}
