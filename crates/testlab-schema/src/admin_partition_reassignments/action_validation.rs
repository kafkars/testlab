//! Reassignment validation bounds exact selections, replica IDs, and result rows.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

const MAX_PARTITIONS: usize = 32;

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    match action {
        ScenarioAction::AlterPartitionReassignments(action) => {
            validate_identity(
                &action.client_id,
                &action.operation_id,
                clients,
                operation_ids,
                problems,
            );
            validate_changes(&action.operation_id, &action.changes, problems);
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
            true
        }
        ScenarioAction::ListPartitionReassignments(action) => {
            validate_identity(
                &action.client_id,
                &action.operation_id,
                clients,
                operation_ids,
                problems,
            );
            validate_listing(action, problems);
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
            true
        }
        _ => false,
    }
}

fn validate_changes(
    operation_id: &OperationId,
    changes: &[crate::PartitionReassignmentChangeSpec],
    problems: &mut Vec<String>,
) {
    if !(1..=MAX_PARTITIONS).contains(&changes.len()) {
        problems.push(format!(
            "admin operation {operation_id} must alter 1 to {MAX_PARTITIONS} partitions"
        ));
    }
    let mut targets = BTreeSet::new();
    for change in changes {
        validate_target(operation_id, &change.topic, change.partition, problems);
        if !targets.insert((&change.topic, change.partition)) {
            problems.push(format!(
                "admin operation {operation_id} repeats a reassignment target"
            ));
        }
        validate_replicas(operation_id, "replacement", &change.replicas, problems);
    }
}

fn validate_listing(action: &crate::ListPartitionReassignmentsAction, problems: &mut Vec<String>) {
    if let Some(targets) = &action.targets {
        if !(1..=MAX_PARTITIONS).contains(&targets.len()) {
            problems.push(format!(
                "admin operation {} must select 1 to {MAX_PARTITIONS} partitions",
                action.operation_id
            ));
        }
        let mut unique = BTreeSet::new();
        for target in targets {
            validate_target(
                &action.operation_id,
                &target.topic,
                target.partition,
                problems,
            );
            if !unique.insert((&target.topic, target.partition)) {
                problems.push(format!(
                    "admin operation {} repeats a listing target",
                    action.operation_id
                ));
            }
        }
    }
    if action.expected_reassignments.len() > MAX_PARTITIONS {
        problems.push(format!(
            "admin operation {} expected_reassignments exceeds {MAX_PARTITIONS} rows",
            action.operation_id
        ));
    }
    let mut identities = BTreeSet::new();
    for row in &action.expected_reassignments {
        validate_target(&action.operation_id, &row.topic, row.partition, problems);
        validate_replicas(&action.operation_id, "current", &row.replicas, problems);
        validate_optional_replicas(
            &action.operation_id,
            "adding",
            &row.adding_replicas,
            problems,
        );
        validate_optional_replicas(
            &action.operation_id,
            "removing",
            &row.removing_replicas,
            problems,
        );
        if !identities.insert((&row.topic, row.partition)) {
            problems.push(format!(
                "admin operation {} repeats an expected reassignment",
                action.operation_id
            ));
        }
    }
    validate_listing_order(action, problems);
}

fn validate_listing_order(
    action: &crate::ListPartitionReassignmentsAction,
    problems: &mut Vec<String>,
) {
    let expected = action
        .expected_reassignments
        .iter()
        .map(|row| (row.topic.as_str(), row.partition))
        .collect::<Vec<_>>();
    let ordered = if let Some(targets) = &action.targets {
        targets
            .iter()
            .map(|target| (target.topic.as_str(), target.partition))
            .filter(|target| expected.contains(target))
            .eq(expected.iter().copied())
    } else {
        expected.windows(2).all(|pair| pair[0] < pair[1])
    };
    if !ordered {
        problems.push(format!(
            "admin operation {} expected reassignments use the wrong result order",
            action.operation_id
        ));
    }
}

fn validate_target(
    operation_id: &OperationId,
    topic: &str,
    partition: i32,
    problems: &mut Vec<String>,
) {
    if topic.is_empty() || topic.len() > 249 || topic.chars().any(char::is_whitespace) {
        problems.push(format!(
            "admin operation {operation_id} has an invalid reassignment topic"
        ));
    }
    if partition < 0 {
        problems.push(format!(
            "admin operation {operation_id} has a negative reassignment partition"
        ));
    }
}

fn validate_replicas(
    operation_id: &OperationId,
    label: &str,
    replicas: &[i32],
    problems: &mut Vec<String>,
) {
    if !(1..=MAX_PARTITIONS).contains(&replicas.len()) {
        problems.push(format!(
            "admin operation {operation_id} {label} replicas must contain 1 to {MAX_PARTITIONS} IDs"
        ));
    }
    validate_replica_ids(operation_id, label, replicas, problems);
}

fn validate_optional_replicas(
    operation_id: &OperationId,
    label: &str,
    replicas: &[i32],
    problems: &mut Vec<String>,
) {
    if replicas.len() > MAX_PARTITIONS {
        problems.push(format!(
            "admin operation {operation_id} {label} replicas exceeds {MAX_PARTITIONS} IDs"
        ));
    }
    validate_replica_ids(operation_id, label, replicas, problems);
}

fn validate_replica_ids(
    operation_id: &OperationId,
    label: &str,
    replicas: &[i32],
    problems: &mut Vec<String>,
) {
    let unique = replicas.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != replicas.len() || replicas.iter().any(|replica| *replica < 0) {
        problems.push(format!(
            "admin operation {operation_id} {label} replicas must be unique nonnegative IDs"
        ));
    }
}
