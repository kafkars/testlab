//! Transaction-listing filters are checked against one stable unfiltered baseline.

use testlab_schema::{
    AdminTransactionsListing, BrokerTransactionsState, ListTransactionsAction, ScenarioAction,
    TransactionListingSnapshot, Violation,
};

use crate::admin::{immediate_after_public, public_after_command};
use crate::admin_transactions::{canonical_list, one};
use crate::index::HistoryIndex;
use crate::index::admin_features::Indexed;
use crate::support::violation;

pub(crate) fn selected(action: &ListTransactionsAction) -> bool {
    !action.state_filters.is_empty()
        || !action.producer_id_filters.is_empty()
        || action.duration_filter_ms.is_some()
        || action.transactional_id_pattern.is_some()
}

pub(crate) fn verify(
    scenario_action: &ScenarioAction,
    action: &ListTransactionsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(scenario_action);
    let baseline = action
        .baseline_operation_id
        .as_ref()
        .and_then(|operation| one(index.admin_features.transactions_listed.get(operation)));
    let public = one(index
        .admin_features
        .transactions_listed
        .get(&action.operation_id));
    let independent = one(index
        .admin_features
        .transactions_observed
        .get(&action.operation_id));
    let matches = index.admin_command_state(scenario_action) == (true, 1)
        && baseline.is_some_and(|baseline| {
            action
                .baseline_operation_id
                .as_ref()
                .is_some_and(|operation| {
                    baseline.value.operation_id == *operation
                        && !baseline.value.transactions.is_empty()
                        && canonical_list(&baseline.value.transactions)
                })
                && window.is_some_and(|(command, _)| baseline.history_sequence < command)
                && public.is_some_and(|public| {
                    public.value.operation_id == action.operation_id
                        && public_after_command(window, public.history_sequence)
                        && independent.is_some_and(|independent| {
                            current_full_matches(baseline, public, independent, window)
                                && selected_rows_match(
                                    action,
                                    &public.value.transactions,
                                    independent,
                                )
                        })
                })
        });
    if matches {
        return;
    }
    violations.push(violation(
        "ADMIN-083",
        format!(
            "admin operation {} expected exact transaction-listing filters to strictly narrow one stable unfiltered baseline",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        evidence(baseline, public, independent),
    ));
}

fn current_full_matches(
    baseline: &Indexed<AdminTransactionsListing>,
    public: &Indexed<AdminTransactionsListing>,
    independent: &Indexed<BrokerTransactionsState>,
    window: Option<crate::admin::AdminCommandWindow>,
) -> bool {
    baseline.value.transactions.len() > public.value.transactions.len()
        && independent.value.operation_id == public.value.operation_id
        && independent
            .value
            .transactions
            .iter()
            .all(|row| row.coordinator_id >= 0)
        && independent
            .value
            .transactions
            .iter()
            .map(|row| &row.transaction)
            .eq(&baseline.value.transactions)
        && immediate_after_public(
            window,
            public.history_sequence,
            independent.history_sequence,
        )
}

fn selected_rows_match(
    action: &ListTransactionsAction,
    actual: &[TransactionListingSnapshot],
    full: &Indexed<BrokerTransactionsState>,
) -> bool {
    actual.len() == action.expected_transactions.len()
        && canonical_list(actual)
        && actual
            .iter()
            .zip(&action.expected_transactions)
            .all(|(actual, expected)| {
                actual.transactional_id == expected.transactional_id
                    && actual.transaction_state == expected.expected_state
                    && full
                        .value
                        .transactions
                        .iter()
                        .any(|row| &row.transaction == actual)
            })
}

fn evidence(
    baseline: Option<&Indexed<AdminTransactionsListing>>,
    public: Option<&Indexed<AdminTransactionsListing>>,
    independent: Option<&Indexed<BrokerTransactionsState>>,
) -> Vec<String> {
    baseline
        .into_iter()
        .chain(public)
        .map(|value| format!("history:{}", value.history_sequence))
        .chain(
            independent
                .into_iter()
                .map(|value| format!("broker-state-observation:{}", value.value.observation)),
        )
        .collect()
}

#[cfg(test)]
#[path = "admin_transaction_filters_test.rs"]
mod tests;
