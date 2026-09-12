//! Group-listing filter validation keeps public selections bounded and unambiguous.

use std::collections::BTreeSet;

use crate::{GroupListingApi, ListConsumerGroupsAction, OperationId};

const MAX_FILTERS_PER_KIND: usize = 32;
const MAX_FILTER_BYTES: usize = 64;

pub(super) fn validate(action: &ListConsumerGroupsAction, problems: &mut Vec<String>) {
    validate_kind(
        &action.operation_id,
        "state_filters",
        &action.state_filters,
        problems,
    );
    validate_kind(
        &action.operation_id,
        "group_type_filters",
        &action.group_type_filters,
        problems,
    );
    validate_kind(
        &action.operation_id,
        "protocol_type_filters",
        &action.protocol_type_filters,
        problems,
    );
    if action.api == GroupListingApi::ConsumerGroups && !action.protocol_type_filters.is_empty() {
        problems.push(format!(
            "admin operation {} protocol_type_filters require api = all_groups",
            action.operation_id
        ));
    }
}

fn validate_kind(
    operation_id: &OperationId,
    field: &str,
    values: &[String],
    problems: &mut Vec<String>,
) {
    if values.len() > MAX_FILTERS_PER_KIND {
        problems.push(format!(
            "admin operation {operation_id} {field} must contain at most {MAX_FILTERS_PER_KIND} entries"
        ));
    }
    let mut unique = BTreeSet::new();
    for value in values {
        if value.is_empty()
            || value.len() > MAX_FILTER_BYTES
            || value.chars().any(char::is_whitespace)
            || !unique.insert(value)
        {
            problems.push(format!(
                "admin operation {operation_id} {field} must contain unique bounded non-whitespace values"
            ));
        }
    }
}
