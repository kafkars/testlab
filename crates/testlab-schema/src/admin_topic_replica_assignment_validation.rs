//! Manual topic assignments must be complete, contiguous, and unambiguous.

use std::collections::BTreeSet;

pub(super) fn validate(action: &crate::CreateTopicAction, problems: &mut Vec<String>) {
    let Some(assignments) = action.replica_assignments.as_deref() else {
        return;
    };
    if assignments.len() != usize::try_from(action.partitions).unwrap_or_default() {
        problems.push(format!(
            "admin operation {} replica_assignments must contain exactly one entry per partition",
            action.operation_id
        ));
    }
    for (expected_index, assignment) in assignments.iter().enumerate() {
        if usize::try_from(assignment.partition_index) != Ok(expected_index) {
            problems.push(format!(
                "admin operation {} replica_assignments must use contiguous caller order from partition 0",
                action.operation_id
            ));
            break;
        }
        if assignment.broker_ids.len()
            != usize::try_from(action.replication_factor).unwrap_or_default()
        {
            problems.push(format!(
                "admin operation {} partition {} broker_ids must match replication_factor",
                action.operation_id, assignment.partition_index
            ));
        }
        let unique = assignment
            .broker_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if assignment.broker_ids.iter().any(|broker| *broker < 0)
            || unique.len() != assignment.broker_ids.len()
        {
            problems.push(format!(
                "admin operation {} partition {} broker_ids must be unique nonnegative IDs",
                action.operation_id, assignment.partition_index
            ));
        }
    }
}
