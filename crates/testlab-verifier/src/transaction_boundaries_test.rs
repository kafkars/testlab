//! Transaction boundary tests pin exact staging and successive public dispositions.

use testlab_schema::{
    AdapterEvent, BatchRecord, OperationId, ProducerId, ScenarioAction, TerminalStatus,
    TransactionDisposition, VisibilityExpectation,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{event, record, scenario, step};

#[test]
fn complete_staged_set_precedes_its_disposition() {
    let operations = operations("first");
    let history = transaction_history(&operations, "transaction-first", 1, 5);
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    crate::transaction_boundaries::verify_staging(
        &operation_id("transaction-first"),
        &operations,
        &index,
        &mut violations,
    );

    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn missing_or_wrong_staging_terminal_fails_declared_set() {
    let operations = operations("first");
    let mut history = transaction_history(&operations, "transaction-first", 1, 5);
    history.retain(|entry| entry.sequence != 4);
    history.push(event(
        4,
        AdapterEvent::OperationTerminal {
            operation_id: operations[1].operation_id.clone(),
            status: TerminalStatus::Acknowledged,
            code: None,
            offset: Some(2),
        },
    ));
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    crate::transaction_boundaries::verify_staging(
        &operation_id("transaction-first"),
        &operations,
        &index,
        &mut violations,
    );

    assert!(violates(&violations, "TXN-004"), "{violations:?}");
}

#[test]
fn duplicate_staging_event_fails_declared_set() {
    let operations = operations("first");
    let mut history = transaction_history(&operations, "transaction-first", 1, 7);
    history.push(event(
        6,
        AdapterEvent::OperationTerminal {
            operation_id: operations[0].operation_id.clone(),
            status: TerminalStatus::TransactionStaged,
            code: None,
            offset: Some(0),
        },
    ));
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    crate::transaction_boundaries::verify_staging(
        &operation_id("transaction-first"),
        &operations,
        &index,
        &mut violations,
    );

    assert!(violates(&violations, "TXN-004"), "{violations:?}");
}

#[test]
fn staging_after_completion_fails_transaction_boundary() {
    let operations = operations("first");
    let history = transaction_history(&operations, "transaction-first", 3, 2);
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    crate::transaction_boundaries::verify_staging(
        &operation_id("transaction-first"),
        &operations,
        &index,
        &mut violations,
    );

    assert!(violates(&violations, "TXN-004"), "{violations:?}");
}

#[test]
fn successive_transactions_on_one_producer_do_not_overlap() {
    let (scenario, operations_a, operations_b) = successive_scenario();
    let mut history = transaction_history(&operations_a, "transaction-first", 1, 5);
    history.extend(transaction_history(
        &operations_b,
        "transaction-second",
        6,
        10,
    ));
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    crate::transaction_boundaries::verify_successive(&scenario, &index, &mut violations);

    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn later_transaction_start_before_prior_completion_fails_boundary() {
    let (scenario, operations_a, operations_b) = successive_scenario();
    let mut history = transaction_history(&operations_a, "transaction-first", 1, 8);
    history.extend(transaction_history(
        &operations_b,
        "transaction-second",
        6,
        12,
    ));
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    crate::transaction_boundaries::verify_successive(&scenario, &index, &mut violations);

    assert!(violates(&violations, "TXN-006"), "{violations:?}");
}

fn successive_scenario() -> (testlab_schema::Scenario, Vec<BatchRecord>, Vec<BatchRecord>) {
    let mut value = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    let producer = producer_id("transactional-producer");
    let operations_a = operations("first");
    let operations_b = operations("second");
    value.steps = vec![
        transaction_step(
            "first-transaction",
            &producer,
            "transaction-first",
            &operations_a,
            TransactionDisposition::Commit,
        ),
        transaction_step(
            "second-transaction",
            &producer,
            "transaction-second",
            &operations_b,
            TransactionDisposition::Abort,
        ),
    ];
    (value, operations_a, operations_b)
}

fn transaction_step(
    step_id: &str,
    producer_id: &ProducerId,
    transaction_id: &str,
    operations: &[BatchRecord],
    disposition: TransactionDisposition,
) -> testlab_schema::ScenarioStep {
    step(
        step_id,
        ScenarioAction::ExecuteTransaction {
            producer_id: producer_id.clone(),
            transaction_id: operation_id(transaction_id),
            operations: operations.to_vec(),
            disposition,
            timeout_ms: 1_000,
        },
    )
}

fn operations(prefix: &str) -> Vec<BatchRecord> {
    ["a", "b"]
        .into_iter()
        .map(|suffix| BatchRecord {
            operation_id: operation_id(&format!("operation-{prefix}-{suffix}")),
            record: record(suffix),
        })
        .collect()
}

fn transaction_history(
    operations: &[BatchRecord],
    transaction_id: &str,
    first_sequence: u64,
    completion_sequence: u64,
) -> Vec<testlab_schema::HistoryEntry> {
    let mut history = Vec::new();
    for (index, operation) in operations.iter().enumerate() {
        let sequence = first_sequence + (index as u64 * 2);
        history.push(event(
            sequence,
            AdapterEvent::OperationAccepted {
                operation_id: operation.operation_id.clone(),
            },
        ));
        history.push(event(
            sequence + 1,
            AdapterEvent::OperationTerminal {
                operation_id: operation.operation_id.clone(),
                status: TerminalStatus::TransactionStaged,
                code: None,
                offset: Some(offset(index)),
            },
        ));
    }
    history.push(event(
        completion_sequence,
        AdapterEvent::TransactionCompleted {
            transaction_id: operation_id(transaction_id),
            disposition: TransactionDisposition::Commit,
        },
    ));
    history
}

fn violates(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}

fn operation_id(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn producer_id(value: &str) -> ProducerId {
    ProducerId::new(value).unwrap_or_else(|error| panic!("producer id: {error}"))
}

fn offset(value: usize) -> i64 {
    i64::try_from(value).unwrap_or_else(|error| panic!("fixture offset: {error}"))
}
