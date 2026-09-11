//! Reassignment verification joins public results to metadata and pinned CLI facts.

use std::collections::BTreeSet;

use testlab_schema::{
    AlterPartitionReassignmentsAction, ListPartitionReassignmentsAction,
    PartitionReassignmentSnapshot, ScenarioAction, Violation,
};

use crate::admin::{immediate_after_public, public_after_command};
use crate::admin_transactions::one;
use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify_partition_reassignments_action(
    scenario_action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    match scenario_action {
        ScenarioAction::AlterPartitionReassignments(action) => {
            verify_alteration(scenario_action, action, index, violations);
        }
        ScenarioAction::ListPartitionReassignments(action) => {
            verify_listing(scenario_action, action, index, violations);
        }
        _ => return false,
    }
    true
}

fn verify_alteration(
    scenario_action: &ScenarioAction,
    action: &AlterPartitionReassignmentsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(scenario_action);
    let public = one(index
        .admin_partition_reassignments
        .altered
        .get(&action.operation_id));
    let independent = one(index
        .admin_partition_reassignments
        .assignments_observed
        .get(&action.operation_id));
    let matches = public.is_some_and(|public| {
        public_after_command(window, public.history_sequence)
            && public.value.operation_id == action.operation_id
            && public.value.throttle_time_ms <= action.timeout_ms
            && public.value.outcomes.len() == action.changes.len()
            && public
                .value
                .outcomes
                .iter()
                .zip(&action.changes)
                .all(|(actual, expected)| {
                    actual.topic == expected.topic
                        && actual.partition == expected.partition
                        && actual.error_code.is_none()
                })
            && independent.is_some_and(|independent| {
                independent.value.operation_id == action.operation_id
                    && independent.value.assignments.len() == action.changes.len()
                    && independent
                        .value
                        .assignments
                        .iter()
                        .zip(&action.changes)
                        .all(|(actual, expected)| {
                            let mut expected_isr = expected.replicas.clone();
                            expected_isr.sort_unstable();
                            actual.topic == expected.topic
                                && actual.partition == expected.partition
                                && actual.replicas == expected.replicas
                                && actual.in_sync_replicas == expected_isr
                                && actual.leader_id >= 0
                                && actual.replicas.contains(&actual.leader_id)
                        })
                    && immediate_after_public(
                        window,
                        public.history_sequence,
                        independent.history_sequence,
                    )
            })
    });
    if !matches {
        violations.push(violation(
            "ADMIN-058",
            format!(
                "admin operation {} expected caller-ordered successful reassignment outcomes and immediate exact converged metadata assignments",
                action.operation_id
            ),
            Some(action.operation_id.clone()),
            references(public, independent),
        ));
    }
}

fn verify_listing(
    scenario_action: &ScenarioAction,
    action: &ListPartitionReassignmentsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(scenario_action);
    let public = one(index
        .admin_partition_reassignments
        .listed
        .get(&action.operation_id));
    let independent = one(index
        .admin_partition_reassignments
        .reassignments_observed
        .get(&action.operation_id));
    let matches = public.is_some_and(|public| {
        public_after_command(window, public.history_sequence)
            && public.value.operation_id == action.operation_id
            && public.value.throttle_time_ms <= action.timeout_ms
            && public.value.reassignments == action.expected_reassignments
            && valid_rows(&public.value.reassignments, action.targets.is_none())
            && independent.is_some_and(|independent| {
                independent.value.operation_id == action.operation_id
                    && valid_rows(&independent.value.reassignments, true)
                    && selected_rows(&independent.value.reassignments, action)
                        .is_some_and(|rows| rows.as_slice() == public.value.reassignments)
                    && immediate_after_public(
                        window,
                        public.history_sequence,
                        independent.history_sequence,
                    )
            })
    });
    if !matches {
        violations.push(violation(
            "ADMIN-059",
            format!(
                "admin operation {} expected exact deterministic public active reassignments matching an immediate pinned Kafka CLI snapshot",
                action.operation_id
            ),
            Some(action.operation_id.clone()),
            references(public, independent),
        ));
    }
}

fn selected_rows(
    rows: &[PartitionReassignmentSnapshot],
    action: &ListPartitionReassignmentsAction,
) -> Option<Vec<PartitionReassignmentSnapshot>> {
    let Some(targets) = &action.targets else {
        return Some(rows.to_vec());
    };
    let mut selected = Vec::new();
    for target in targets {
        if let Some(row) = rows
            .iter()
            .find(|row| row.topic == target.topic && row.partition == target.partition)
        {
            selected.push(row.clone());
        }
    }
    Some(selected)
}

fn valid_rows(rows: &[PartitionReassignmentSnapshot], canonical: bool) -> bool {
    rows.iter().all(valid_row)
        && (!canonical
            || rows.windows(2).all(|pair| {
                pair[0].topic.as_bytes() < pair[1].topic.as_bytes()
                    || (pair[0].topic == pair[1].topic && pair[0].partition < pair[1].partition)
            }))
}

fn valid_row(row: &PartitionReassignmentSnapshot) -> bool {
    !row.topic.is_empty()
        && row.partition >= 0
        && valid_ids(&row.replicas, false)
        && valid_ids(&row.adding_replicas, true)
        && valid_ids(&row.removing_replicas, true)
}

fn valid_ids(values: &[i32], allow_empty: bool) -> bool {
    (allow_empty || !values.is_empty())
        && values.iter().all(|value| *value >= 0)
        && values.iter().copied().collect::<BTreeSet<_>>().len() == values.len()
}

fn references<T, U>(
    public: Option<&crate::index::admin_features::Indexed<T>>,
    independent: Option<&crate::index::admin_features::Indexed<U>>,
) -> Vec<String> {
    public
        .map(|value| format!("history:{}", value.history_sequence))
        .into_iter()
        .chain(independent.map(|value| format!("history:{}", value.history_sequence)))
        .collect()
}
