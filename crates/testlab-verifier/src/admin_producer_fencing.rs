//! Producer fencing joins caller-ordered public identities to Kafka CLI state.

use testlab_schema::{FenceProducersAction, ScenarioAction, Violation};

use crate::admin::{immediate_after_public, public_after_command};
use crate::admin_transactions::{contiguous, one, references, valid_description, valid_name};
use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(
    scenario_action: &ScenarioAction,
    action: &FenceProducersAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(scenario_action);
    let public = one(index
        .admin_features
        .producers_fenced
        .get(&action.operation_id));
    let independent = index
        .admin_features
        .transaction_states_observed
        .get(&action.operation_id);
    let matches = public.is_some_and(|public| {
        public_after_command(window, public.history_sequence)
            && public.value.operation_id == action.operation_id
            && public.value.throttle_time_ms <= action.timeout_ms
            && public.value.producers.len() == action.producers.len()
            && public
                .value
                .producers
                .iter()
                .zip(&action.producers)
                .all(|(actual, expected)| {
                    valid_name(&actual.transactional_id)
                        && actual.transactional_id == expected.transactional_id
                        && actual.producer_id >= 0
                        && actual.producer_epoch >= 0
                })
            && independent.is_some_and(|independent| {
                independent.len() == action.producers.len()
                    && contiguous(independent.iter().map(|value| value.value.observation))
                    && contiguous(independent.iter().map(|value| value.history_sequence))
                    && independent
                        .iter()
                        .zip(&public.value.producers)
                        .zip(&action.producers)
                        .all(|((observed, actual), expected)| {
                            let transaction = &observed.value.transaction;
                            observed.value.operation_id == action.operation_id
                                && observed.value.coordinator_id >= 0
                                && valid_description(transaction)
                                && transaction.transactional_id == actual.transactional_id
                                && transaction.producer_id == actual.producer_id
                                && transaction.producer_epoch == actual.producer_epoch
                                && transaction.transaction_state == expected.expected_state
                                && u64::try_from(transaction.transaction_timeout_ms)
                                    .is_ok_and(|timeout| timeout <= action.timeout_ms)
                                && transaction.transaction_start_time_ms.is_some()
                                    == expected.expected_start_time_present
                                && transaction.topics == expected.expected_topics
                                && immediate_after_public(
                                    window,
                                    public.history_sequence,
                                    observed.history_sequence,
                                )
                        })
            })
    });
    if matches {
        return;
    }
    violations.push(violation(
        "ADMIN-057",
        format!(
            "admin operation {} expected caller-ordered producer fencing identities exactly matching immediate Kafka CLI transaction state",
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
