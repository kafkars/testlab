//! Manual partition increases retain one valid broker list per new partition.

use std::collections::BTreeSet;

pub(super) fn validate(action: &crate::CreatePartitionsAction, problems: &mut Vec<String>) {
    let Some(assignments) = action.replica_assignments.as_deref() else {
        return;
    };
    let Some(current_count) = action.expected_current_count else {
        return;
    };
    if current_count > 0 && current_count < action.total_count {
        let expected = usize::try_from(action.total_count - current_count).unwrap_or_default();
        if assignments.len() != expected {
            problems.push(format!(
                "admin operation {} replica_assignments must contain exactly one entry per newly added partition",
                action.operation_id
            ));
        }
    }
    for (offset, broker_ids) in assignments.iter().enumerate() {
        if broker_ids.is_empty() || broker_ids.len() > 100 {
            problems.push(format!(
                "admin operation {} new partition offset {offset} broker_ids must contain 1 to 100 entries",
                action.operation_id
            ));
        }
        let unique = broker_ids.iter().copied().collect::<BTreeSet<_>>();
        if broker_ids.iter().any(|broker| *broker < 0) || unique.len() != broker_ids.len() {
            problems.push(format!(
                "admin operation {} new partition offset {offset} broker_ids must be unique nonnegative IDs",
                action.operation_id
            ));
        }
    }
}
