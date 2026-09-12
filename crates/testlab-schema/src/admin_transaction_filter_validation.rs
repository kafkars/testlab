//! Transaction-listing filter intent stays bounded and requires an explicit baseline.

use std::collections::BTreeSet;

use crate::{ListTransactionsAction, OperationId};

const MAX_TRANSACTIONS: usize = 32;
const MAX_FILTERS_PER_KIND: usize = 32;
const MAX_STATE_FILTER_BYTES: usize = 64;
const MAX_PATTERN_BYTES: usize = 1024;

pub(crate) fn filtered(action: &ListTransactionsAction) -> bool {
    !action.state_filters.is_empty()
        || !action.producer_id_filters.is_empty()
        || action.duration_filter_ms.is_some()
        || action.transactional_id_pattern.is_some()
}

pub(crate) fn validate(action: &ListTransactionsAction, problems: &mut Vec<String>) {
    validate_strings(
        &action.operation_id,
        "state_filters",
        &action.state_filters,
        problems,
    );
    if action.producer_id_filters.len() > MAX_FILTERS_PER_KIND {
        problems.push(format!(
            "admin operation {} producer_id_filters must contain at most {MAX_FILTERS_PER_KIND} entries",
            action.operation_id
        ));
    }
    let mut producer_ids = BTreeSet::new();
    if action
        .producer_id_filters
        .iter()
        .any(|producer_id| !producer_ids.insert(*producer_id))
    {
        problems.push(format!(
            "admin operation {} producer_id_filters must contain unique values",
            action.operation_id
        ));
    }
    if action
        .duration_filter_ms
        .is_some_and(|duration| duration > i64::MAX as u64)
    {
        problems.push(format!(
            "admin operation {} duration_filter_ms must fit Kafka's signed 64-bit field",
            action.operation_id
        ));
    }
    if action
        .transactional_id_pattern
        .as_deref()
        .is_some_and(|pattern| pattern.is_empty() || pattern.len() > MAX_PATTERN_BYTES)
    {
        problems.push(format!(
            "admin operation {} transactional_id_pattern must contain 1 to {MAX_PATTERN_BYTES} bytes",
            action.operation_id
        ));
    }
    validate_expectations(action, problems);
}

fn validate_expectations(action: &ListTransactionsAction, problems: &mut Vec<String>) {
    if filtered(action) {
        if action.expected_transactions.len() > MAX_TRANSACTIONS {
            problems.push(format!(
                "admin operation {} must select at most {MAX_TRANSACTIONS} filtered transactions",
                action.operation_id
            ));
        }
        if action.baseline_operation_id.is_none() {
            problems.push(format!(
                "admin operation {} filtered listing requires baseline_operation_id",
                action.operation_id
            ));
        }
    } else {
        if !(1..=MAX_TRANSACTIONS).contains(&action.expected_transactions.len()) {
            problems.push(format!(
                "admin operation {} must select 1 to {MAX_TRANSACTIONS} transactions",
                action.operation_id
            ));
        }
        if action.baseline_operation_id.is_some() {
            problems.push(format!(
                "admin operation {} unfiltered listing must omit baseline_operation_id",
                action.operation_id
            ));
        }
    }
}

fn validate_strings(
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
    if values.iter().any(|value| {
        value.is_empty()
            || value.len() > MAX_STATE_FILTER_BYTES
            || value.chars().any(char::is_whitespace)
            || !unique.insert(value)
    }) {
        problems.push(format!(
            "admin operation {operation_id} {field} must contain unique bounded non-whitespace values"
        ));
    }
}
