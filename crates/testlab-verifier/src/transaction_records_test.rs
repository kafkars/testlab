//! Transaction record tests pin set visibility, fidelity, coordinates, and order.

use testlab_schema::{
    AdapterEvent, BatchRecord, BrokerObservation, ByteString, HeaderSpec, OperationId, RecordSpec,
    TerminalStatus, TransactionDisposition,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::event;
use crate::verify_index::observations_by_operation;

#[test]
fn committed_multi_record_set_matches_exactly() {
    let operations = operations();
    let history = staged_history(&operations, &[4, 5, 2]);
    let observations = observations(&operations, &[4, 5, 2]);

    let violations = verify_set(
        &operations,
        TransactionDisposition::Commit,
        &history,
        &observations,
    );

    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn missing_or_duplicate_committed_member_fails_atomic_visibility() {
    let operations = operations();
    let history = staged_history(&operations, &[4, 5, 2]);
    let mut observations = observations(&operations, &[4, 5, 2]);
    observations.remove(1);
    let mut duplicate = observations[0].clone();
    duplicate.observation = 9;
    observations.push(duplicate);

    let violations = verify_set(
        &operations,
        TransactionDisposition::Commit,
        &history,
        &observations,
    );

    assert!(violates(&violations, "TXN-002"), "{violations:?}");
}

#[test]
fn committed_bytes_must_match_broker_truth() {
    let operations = operations();
    let history = staged_history(&operations, &[4, 5, 2]);
    let mut observations = observations(&operations, &[4, 5, 2]);
    observations[0].record.value = Some(ByteString::hex("ff"));
    observations[0].digest = digest(&observations[0].record);

    let violations = verify_set(
        &operations,
        TransactionDisposition::Commit,
        &history,
        &observations,
    );

    assert!(violates(&violations, "TXN-005"), "{violations:?}");
}

#[test]
fn committed_topic_partition_must_match_broker_truth() {
    let operations = operations();
    let history = staged_history(&operations, &[4, 5, 2]);
    let mut observations = observations(&operations, &[4, 5, 2]);
    observations[0].record.partition = 1;
    observations[0].digest = digest(&observations[0].record);

    let violations = verify_set(
        &operations,
        TransactionDisposition::Commit,
        &history,
        &observations,
    );

    assert!(violates(&violations, "TXN-005"), "{violations:?}");
}

#[test]
fn committed_public_offset_must_match_broker_truth() {
    let operations = operations();
    let history = staged_history(&operations, &[9, 5, 2]);
    let observations = observations(&operations, &[4, 5, 2]);

    let violations = verify_set(
        &operations,
        TransactionDisposition::Commit,
        &history,
        &observations,
    );

    assert!(violates(&violations, "TXN-005"), "{violations:?}");
}

#[test]
fn committed_same_partition_records_retain_declared_order() {
    let operations = operations();
    let history = staged_history(&operations, &[5, 4, 2]);
    let observations = observations(&operations, &[5, 4, 2]);

    let violations = verify_set(
        &operations,
        TransactionDisposition::Commit,
        &history,
        &observations,
    );

    assert!(violates(&violations, "TXN-005"), "{violations:?}");
}

#[test]
fn aborted_set_requires_only_complete_read_committed_absence() {
    let operations = operations();
    let history = staged_history(&operations, &[41, 42, 11]);

    let violations = verify_set(&operations, TransactionDisposition::Abort, &history, &[]);

    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn any_aborted_member_visible_to_read_committed_fails_atomicity() {
    let operations = operations();
    let history = staged_history(&operations, &[41, 42, 11]);
    let observations = observations(&operations[..1], &[41]);

    let violations = verify_set(
        &operations,
        TransactionDisposition::Abort,
        &history,
        &observations,
    );

    assert!(violates(&violations, "TXN-002"), "{violations:?}");
}

fn verify_set(
    operations: &[BatchRecord],
    disposition: TransactionDisposition,
    history: &[testlab_schema::HistoryEntry],
    observations: &[BrokerObservation],
) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let observed = observations_by_operation(observations);
    let mut violations = Vec::new();
    crate::transaction_records::verify(
        &id("transaction-set"),
        operations,
        disposition,
        &index,
        &observed,
        &mut violations,
    );
    violations
}

fn operations() -> Vec<BatchRecord> {
    vec![
        operation("operation-a", "transaction-a", 0, 1, None),
        operation(
            "operation-b",
            "transaction-a",
            0,
            2,
            Some(ByteString::hex("")),
        ),
        operation(
            "operation-c",
            "transaction-b",
            1,
            3,
            Some(ByteString::hex("00ff")),
        ),
    ]
}

fn operation(
    operation_id: &str,
    topic: &str,
    partition: i32,
    sequence: u64,
    value: Option<ByteString>,
) -> BatchRecord {
    BatchRecord {
        operation_id: id(operation_id),
        record: RecordSpec {
            topic: topic.to_owned(),
            partition,
            sequence,
            key: Some(ByteString::hex("")),
            value,
            headers: vec![
                HeaderSpec {
                    name: "trace".to_owned(),
                    value: None,
                },
                HeaderSpec {
                    name: "trace".to_owned(),
                    value: Some(ByteString::hex("00ff")),
                },
            ],
        },
    }
}

fn staged_history(
    operations: &[BatchRecord],
    offsets: &[i64],
) -> Vec<testlab_schema::HistoryEntry> {
    operations
        .iter()
        .zip(offsets)
        .enumerate()
        .map(|(index, (operation, offset))| {
            event(
                index as u64,
                AdapterEvent::OperationTerminal {
                    operation_id: operation.operation_id.clone(),
                    status: TerminalStatus::TransactionStaged,
                    code: None,
                    offset: Some(*offset),
                },
            )
        })
        .collect()
}

fn observations(operations: &[BatchRecord], offsets: &[i64]) -> Vec<BrokerObservation> {
    operations
        .iter()
        .zip(offsets)
        .enumerate()
        .map(|(index, (operation, offset))| BrokerObservation {
            observation: index as u64,
            offset: *offset,
            operation_id: operation.operation_id.clone(),
            digest: digest(&operation.record),
            record: operation.record.clone(),
        })
        .collect()
}

fn digest(record: &RecordSpec) -> String {
    record
        .digest()
        .unwrap_or_else(|error| panic!("record digest: {error}"))
}

fn violates(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}

fn id(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
