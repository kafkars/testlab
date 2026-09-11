//! Transaction discovery joins public Admin results to pinned Kafka CLI snapshots.

use testlab_schema::{
    DescribeTransactionsAction, ListTransactionsAction, ScenarioAction,
    TransactionDescriptionSnapshot, TransactionListingSnapshot, TransactionTopicSnapshot,
    Violation,
};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::index::admin_features::Indexed;
use crate::support::violation;

pub(crate) fn verify_transactions_action(
    scenario_action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    match scenario_action {
        ScenarioAction::ListTransactions(action) => {
            verify_listing(scenario_action, action, index, violations);
        }
        ScenarioAction::DescribeTransactions(action) => {
            verify_descriptions(scenario_action, action, index, violations);
        }
        _ => return false,
    }
    true
}

fn verify_listing(
    scenario_action: &ScenarioAction,
    action: &ListTransactionsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(scenario_action);
    let public = one(index
        .admin_features
        .transactions_listed
        .get(&action.operation_id));
    let independent = one(index
        .admin_features
        .transactions_observed
        .get(&action.operation_id));
    let matches = public.is_some_and(|public| {
        public_after_command(window, public.history_sequence)
            && public.value.operation_id == action.operation_id
            && public.value.transactions.len() == action.expected_transactions.len()
            && canonical_list(&public.value.transactions)
            && public
                .value
                .transactions
                .iter()
                .zip(&action.expected_transactions)
                .all(|(actual, expected)| {
                    actual.transactional_id == expected.transactional_id
                        && actual.transaction_state == expected.expected_state
                })
            && independent.is_some_and(|independent| {
                independent.value.operation_id == action.operation_id
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
                        .eq(&public.value.transactions)
                    && immediate_after_public(
                        window,
                        public.history_sequence,
                        independent.history_sequence,
                    )
            })
    });
    if matches {
        return;
    }
    violations.push(violation(
        "ADMIN-052",
        format!(
            "admin operation {} expected one canonical transaction listing exactly matching one immediate Kafka CLI snapshot",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        references(
            public,
            independent
                .into_iter()
                .map(|value| value.value.observation),
        ),
    ));
}

fn verify_descriptions(
    scenario_action: &ScenarioAction,
    action: &DescribeTransactionsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(scenario_action);
    let public = one(index
        .admin_features
        .transactions_described
        .get(&action.operation_id));
    let independent = index
        .admin_features
        .transaction_states_observed
        .get(&action.operation_id);
    let matches = public.is_some_and(|public| {
        public_after_command(window, public.history_sequence)
            && public.value.operation_id == action.operation_id
            && public.value.transactions.len() == action.transactions.len()
            && public
                .value
                .transactions
                .iter()
                .zip(&action.transactions)
                .all(|(actual, expected)| {
                    valid_description(actual)
                        && actual.transactional_id == expected.transactional_id
                        && actual.transaction_state == expected.expected_state
                        && actual.transaction_timeout_ms == expected.expected_transaction_timeout_ms
                        && actual.transaction_start_time_ms.is_some()
                            == expected.expected_start_time_present
                        && actual.topics == expected.expected_topics
                })
            && independent.is_some_and(|independent| {
                independent.len() == action.transactions.len()
                    && contiguous(independent.iter().map(|value| value.value.observation))
                    && contiguous(independent.iter().map(|value| value.history_sequence))
                    && independent.iter().zip(&public.value.transactions).all(
                        |(observed, actual)| {
                            observed.value.operation_id == action.operation_id
                                && observed.value.coordinator_id >= 0
                                && &observed.value.transaction == actual
                                && immediate_after_public(
                                    window,
                                    public.history_sequence,
                                    observed.history_sequence,
                                )
                        },
                    )
            })
    });
    if matches {
        return;
    }
    violations.push(violation(
        "ADMIN-053",
        format!(
            "admin operation {} expected caller-ordered exact transaction descriptions and one immediate Kafka CLI snapshot per ID",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        references(
            public,
            independent
                .into_iter()
                .flatten()
                .map(|value| value.value.observation),
        ),
    ));
}

fn canonical_list(transactions: &[TransactionListingSnapshot]) -> bool {
    transactions.iter().all(|transaction| {
        valid_name(&transaction.transactional_id)
            && transaction.producer_id >= 0
            && valid_name(&transaction.transaction_state)
    }) && transactions
        .windows(2)
        .all(|pair| pair[0].transactional_id.as_bytes() < pair[1].transactional_id.as_bytes())
}

fn valid_description(transaction: &TransactionDescriptionSnapshot) -> bool {
    valid_name(&transaction.transactional_id)
        && valid_name(&transaction.transaction_state)
        && transaction.transaction_timeout_ms > 0
        && transaction
            .transaction_start_time_ms
            .is_none_or(|timestamp| timestamp >= 0)
        && transaction.producer_id >= 0
        && transaction.producer_epoch >= 0
        && canonical_topics(&transaction.topics)
}

fn canonical_topics(topics: &[TransactionTopicSnapshot]) -> bool {
    topics.iter().all(|topic| {
        valid_name(&topic.topic)
            && !topic.partitions.is_empty()
            && topic.partitions.iter().all(|partition| *partition >= 0)
            && topic.partitions.windows(2).all(|pair| pair[0] < pair[1])
    }) && topics
        .windows(2)
        .all(|pair| pair[0].topic.as_bytes() < pair[1].topic.as_bytes())
}

fn valid_name(value: &str) -> bool {
    !value.is_empty() && !value.chars().any(char::is_whitespace)
}

fn contiguous(values: impl Iterator<Item = u64>) -> bool {
    let mut previous: Option<u64> = None;
    for value in values {
        if previous.is_some_and(|previous| value != previous.saturating_add(1)) {
            return false;
        }
        previous = Some(value);
    }
    true
}

fn one<T>(values: Option<&Vec<Indexed<T>>>) -> Option<&Indexed<T>> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}

fn references<T>(
    public: Option<&Indexed<T>>,
    observations: impl Iterator<Item = u64>,
) -> Vec<String> {
    public
        .map(|value| format!("history:{}", value.history_sequence))
        .into_iter()
        .chain(observations.map(|value| format!("broker-state-observation:{value}")))
        .collect()
}
