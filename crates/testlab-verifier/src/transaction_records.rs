//! Transaction record verification proves atomic visibility, fidelity, and partition order.

use std::collections::BTreeMap;

use testlab_schema::{
    BatchRecord, BrokerObservation, OperationId, TransactionDisposition, Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(
    transaction_id: &OperationId,
    operations: &[BatchRecord],
    disposition: TransactionDisposition,
    index: &HistoryIndex,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    verify_atomic_visibility(
        transaction_id,
        operations,
        disposition,
        observed,
        violations,
    );
    if disposition == TransactionDisposition::Commit {
        verify_committed_records(transaction_id, operations, index, observed, violations);
        verify_partition_order(transaction_id, operations, observed, violations);
    }
}

fn verify_atomic_visibility(
    transaction_id: &OperationId,
    operations: &[BatchRecord],
    disposition: TransactionDisposition,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    let expected = usize::from(disposition == TransactionDisposition::Commit);
    let mismatched = operations
        .iter()
        .filter(|operation| observed.get(&operation.operation_id).map_or(0, Vec::len) != expected)
        .collect::<Vec<_>>();
    if mismatched.is_empty() {
        return;
    }
    let mut evidence = mismatched
        .iter()
        .map(|operation| format!("scenario:operation:{}", operation.operation_id))
        .collect::<Vec<_>>();
    evidence.extend(mismatched.iter().flat_map(|operation| {
        observation_references(observed.get(&operation.operation_id).map(Vec::as_slice))
    }));
    violations.push(violation(
        "TXN-002",
        format!(
            "transaction {transaction_id} expected {expected} read-committed observation(s) for every declared operation; {} of {} differed",
            mismatched.len(),
            operations.len()
        ),
        Some(transaction_id.clone()),
        evidence,
    ));
}

fn verify_committed_records(
    transaction_id: &OperationId,
    operations: &[BatchRecord],
    index: &HistoryIndex,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    for operation in operations {
        let Some([observation]) = observed.get(&operation.operation_id).map(Vec::as_slice) else {
            continue;
        };
        let expected_digest = operation.record.digest();
        let observed_digest = observation.record.digest();
        let exact_record = expected_digest.as_ref().is_ok_and(|expected| {
            observed_digest
                .as_ref()
                .is_ok_and(|actual| expected == actual && observation.digest == *actual)
        });
        let exact_offset = index
            .terminals
            .get(&operation.operation_id)
            .and_then(|terminals| terminals.as_slice().first())
            .is_some_and(|terminal| {
                index
                    .terminals
                    .get(&operation.operation_id)
                    .is_some_and(|terminals| terminals.len() == 1)
                    && terminal.offset == Some(observation.offset)
            });
        if exact_record && exact_offset {
            continue;
        }
        let mut evidence = vec![format!("broker-observation:{}", observation.observation)];
        evidence.extend(
            index
                .terminals
                .get(&operation.operation_id)
                .into_iter()
                .flatten()
                .map(|terminal| format!("history:{}", terminal.history_sequence)),
        );
        violations.push(violation(
            "TXN-005",
            format!(
                "committed transaction {transaction_id} operation {} did not match its declared bytes and independent coordinates",
                operation.operation_id
            ),
            Some(operation.operation_id.clone()),
            evidence,
        ));
    }
}

fn verify_partition_order(
    transaction_id: &OperationId,
    operations: &[BatchRecord],
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    let mut partitions: BTreeMap<(&str, i32), Vec<&OperationId>> = BTreeMap::new();
    for operation in operations {
        partitions
            .entry((&operation.record.topic, operation.record.partition))
            .or_default()
            .push(&operation.operation_id);
    }
    for operation_ids in partitions.values() {
        for pair in operation_ids.windows(2) {
            let Some([before]) = observed.get(pair[0]).map(Vec::as_slice) else {
                continue;
            };
            let Some([after]) = observed.get(pair[1]).map(Vec::as_slice) else {
                continue;
            };
            if before.offset < after.offset {
                continue;
            }
            violations.push(violation(
                "TXN-005",
                format!(
                    "committed transaction {transaction_id} did not preserve declared per-partition order from {} to {}",
                    pair[0], pair[1]
                ),
                Some((*pair[1]).clone()),
                vec![
                    format!("broker-observation:{}", before.observation),
                    format!("broker-observation:{}", after.observation),
                ],
            ));
        }
    }
}

fn observation_references(observations: Option<&[&BrokerObservation]>) -> Vec<String> {
    observations
        .into_iter()
        .flatten()
        .map(|observation| format!("broker-observation:{}", observation.observation))
        .collect()
}
