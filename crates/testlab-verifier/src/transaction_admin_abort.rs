//! Admin partition-abort verification binds public state transition to independent truth.

use testlab_schema::{
    AdapterCommand, AdminProducersDescription, BrokerProducersState, ProducerStateSnapshot,
    ScenarioAction, TransactionDisposition, Violation,
};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::index::admin_features::Indexed;
use crate::support::violation;

pub(crate) fn verify(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let ScenarioAction::ExecuteTransaction {
        transaction_id,
        operations,
        disposition: TransactionDisposition::AdminPartitionAbort,
        ..
    } = action
    else {
        return;
    };
    let Ok(singleton) = <&[testlab_schema::BatchRecord; 1]>::try_from(operations.as_slice()) else {
        fail(transaction_id, index, violations);
        return;
    };
    let [operation] = singleton;
    let before = index
        .admin_features
        .producers_described
        .get(&operation.operation_id);
    let after = index.admin_features.producers_described.get(transaction_id);
    let independent = index.admin_features.producers_observed.get(transaction_id);
    let completion = index.transactions_completed.get(transaction_id);
    if exact(
        action,
        operation,
        before,
        after,
        independent,
        completion,
        index,
    ) {
        return;
    }
    violations.push(violation(
        "ADMIN-075",
        format!(
            "admin partition abort {transaction_id} expected one exact command, an open-to-cleared public producer transition before transaction cleanup, and one immediate matching cleared Kafka CLI snapshot"
        ),
        Some(transaction_id.clone()),
        evidence(before, after, independent, completion),
    ));
}

#[allow(
    clippy::too_many_arguments,
    reason = "the proof joins each distinct public and independent evidence edge"
)]
fn exact(
    action: &ScenarioAction,
    operation: &testlab_schema::BatchRecord,
    before: Option<&Vec<Indexed<AdminProducersDescription>>>,
    after: Option<&Vec<Indexed<AdminProducersDescription>>>,
    independent: Option<&Vec<Indexed<BrokerProducersState>>>,
    completion: Option<&Vec<crate::index::IndexedTransactionCompletion>>,
    index: &HistoryIndex,
) -> bool {
    let (Some([before]), Some([after]), Some([independent]), Some([completion]), Some(window)) = (
        before.map(Vec::as_slice),
        after.map(Vec::as_slice),
        independent.map(Vec::as_slice),
        completion.map(Vec::as_slice),
        command_window(action, index),
    ) else {
        return false;
    };
    let Ok(before_singleton) =
        <&[ProducerStateSnapshot; 1]>::try_from(before.value.producers.as_slice())
    else {
        return false;
    };
    let Ok(after_singleton) =
        <&[ProducerStateSnapshot; 1]>::try_from(after.value.producers.as_slice())
    else {
        return false;
    };
    let [before_producer] = before_singleton;
    let [after_producer] = after_singleton;
    let transaction_id = operation_id(action);
    same_target(&before.value, operation)
        && after.value.operation_id == *transaction_id
        && after.value.topic == operation.record.topic
        && after.value.partition == operation.record.partition
        && crate::admin_producers::canonical(&before.value.producers)
        && cleared(before_producer, after_producer)
        && independent_confirms_cleared(&after.value, after_producer, &independent.value)
        && completion.disposition == TransactionDisposition::AdminPartitionAbort
        && public_after_command(Some(window), before.history_sequence)
        && before.history_sequence < after.history_sequence
        && after.history_sequence < completion.history_sequence
        && immediate_after_public(
            Some(window),
            completion.history_sequence,
            independent.history_sequence,
        )
}

fn command_window(action: &ScenarioAction, index: &HistoryIndex) -> Option<AdminCommandWindow> {
    let expected = expected_command(action)?;
    let transaction_id = operation_id(action);
    let mut matching = index.commands.iter().filter(|(_, _, command)| {
        matches!(command, AdapterCommand::ExecuteTransaction { transaction_id: actual, .. }
            if actual == transaction_id)
    });
    let (sequence, _, command) = matching.next()?;
    if command != &expected || matching.next().is_some() {
        return None;
    }
    let next = index
        .commands
        .iter()
        .map(|(candidate, _, _)| *candidate)
        .filter(|candidate| candidate > sequence)
        .min();
    Some((*sequence, next))
}

fn expected_command(action: &ScenarioAction) -> Option<AdapterCommand> {
    let ScenarioAction::ExecuteTransaction {
        producer_id,
        transaction_id,
        operations,
        disposition,
        timeout_ms,
    } = action
    else {
        return None;
    };
    Some(AdapterCommand::ExecuteTransaction {
        producer_id: producer_id.clone(),
        transaction_id: transaction_id.clone(),
        operations: operations.clone(),
        disposition: *disposition,
        timeout_ms: *timeout_ms,
    })
}

fn same_target(value: &AdminProducersDescription, operation: &testlab_schema::BatchRecord) -> bool {
    value.operation_id == operation.operation_id
        && value.topic == operation.record.topic
        && value.partition == operation.record.partition
}

fn cleared(before: &ProducerStateSnapshot, after: &ProducerStateSnapshot) -> bool {
    before.current_transaction_start_offset.is_some()
        && after.current_transaction_start_offset.is_none()
        && after.producer_id == before.producer_id
        && after.producer_epoch == before.producer_epoch
        && after.last_sequence == before.last_sequence
        && after.last_timestamp >= before.last_timestamp
        && after.coordinator_epoch == before.coordinator_epoch
}

fn independent_confirms_cleared(
    public: &AdminProducersDescription,
    public_producer: &ProducerStateSnapshot,
    independent: &BrokerProducersState,
) -> bool {
    let [producer] = independent.producers.as_slice() else {
        return false;
    };
    public.operation_id == independent.operation_id
        && public.topic == independent.topic
        && public.partition == independent.partition
        && crate::admin_producers::canonical(&independent.producers)
        && producer.producer_id == public_producer.producer_id
        && producer.producer_epoch == public_producer.producer_epoch
        && producer.last_sequence == public_producer.last_sequence
        && producer.last_timestamp >= public_producer.last_timestamp
        && producer.coordinator_epoch == public_producer.coordinator_epoch
        && producer.current_transaction_start_offset.is_none()
}

fn operation_id(action: &ScenarioAction) -> &testlab_schema::OperationId {
    let ScenarioAction::ExecuteTransaction { transaction_id, .. } = action else {
        unreachable!("admin abort verifier only accepts transaction actions")
    };
    transaction_id
}

fn evidence(
    before: Option<&Vec<Indexed<AdminProducersDescription>>>,
    after: Option<&Vec<Indexed<AdminProducersDescription>>>,
    independent: Option<&Vec<Indexed<BrokerProducersState>>>,
    completion: Option<&Vec<crate::index::IndexedTransactionCompletion>>,
) -> Vec<String> {
    before
        .into_iter()
        .flatten()
        .chain(after.into_iter().flatten())
        .map(|value| format!("history:{}", value.history_sequence))
        .chain(
            completion
                .into_iter()
                .flatten()
                .map(|value| format!("history:{}", value.history_sequence)),
        )
        .chain(
            independent
                .into_iter()
                .flatten()
                .map(|value| format!("broker-state-observation:{}", value.value.observation)),
        )
        .collect()
}

fn fail(
    transaction_id: &testlab_schema::OperationId,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    violations.push(violation(
        "ADMIN-075",
        format!("admin partition abort {transaction_id} did not name exactly one staged record"),
        Some(transaction_id.clone()),
        index
            .transactions_completed
            .get(transaction_id)
            .into_iter()
            .flatten()
            .map(|value| format!("history:{}", value.history_sequence))
            .collect(),
    ));
}
