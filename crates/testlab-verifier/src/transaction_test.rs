//! Transaction verifier tests pin commit visibility and abort isolation.

use testlab_schema::{
    AdapterEvent, BatchRecord, Capability, OperationAssertion, OperationId, ProducerId,
    ScenarioAction, TerminalStatus, TransactionDisposition, VisibilityExpectation,
};

use super::verify;
use crate::verify_fixture::{adapter, event, history, observation, record, scenario, step};

#[test]
fn committed_transaction_is_visible_exactly_once() {
    let (scenario, operation_id, transaction_id, producer_id) =
        transaction_scenario(TransactionDisposition::Commit);
    let mut events = transaction_history(
        operation_id.clone(),
        transaction_id,
        producer_id,
        TransactionDisposition::Commit,
    );
    let mut observed = observation(1, "transaction");
    observed.operation_id = operation_id;
    observed.record = record("transaction");
    observed.digest = observed
        .record
        .digest()
        .unwrap_or_else(|error| panic!("record digest: {error}"));
    let mut descriptor = adapter();
    descriptor.capabilities.insert(Capability::Transactions);
    set_ready_descriptor(&mut events, descriptor.clone());
    let observations = [observation(0, "value"), observed];

    let verdict = verify(&scenario, &descriptor, &events, &observations);

    assert!(verdict.is_passed(), "{verdict:?}");
}

#[test]
fn aborted_transaction_visibility_fails_isolation() {
    let (scenario, operation_id, transaction_id, producer_id) =
        transaction_scenario(TransactionDisposition::Abort);
    let mut events = transaction_history(
        operation_id.clone(),
        transaction_id,
        producer_id,
        TransactionDisposition::Abort,
    );
    let mut leaked = observation(1, "transaction");
    leaked.operation_id = operation_id;
    leaked.record = record("transaction");
    leaked.digest = leaked
        .record
        .digest()
        .unwrap_or_else(|error| panic!("record digest: {error}"));
    let mut descriptor = adapter();
    descriptor.capabilities.insert(Capability::Transactions);
    set_ready_descriptor(&mut events, descriptor.clone());
    let observations = [observation(0, "value"), leaked];

    let verdict = verify(&scenario, &descriptor, &events, &observations);

    assert!(
        verdict
            .violations
            .iter()
            .any(|value| value.contract_id.as_str() == "TXN-002")
    );
}

#[test]
fn wrong_public_disposition_fails_exact_completion() {
    let (scenario, operation_id, transaction_id, producer_id) =
        transaction_scenario(TransactionDisposition::Commit);
    let mut events = transaction_history(
        operation_id.clone(),
        transaction_id,
        producer_id,
        TransactionDisposition::Abort,
    );
    let mut observed = observation(1, "transaction");
    observed.operation_id = operation_id;
    observed.record = record("transaction");
    observed.digest = observed
        .record
        .digest()
        .unwrap_or_else(|error| panic!("record digest: {error}"));
    let mut descriptor = adapter();
    descriptor.capabilities.insert(Capability::Transactions);
    set_ready_descriptor(&mut events, descriptor.clone());

    let verdict = verify(&scenario, &descriptor, &events, &[observed]);

    assert!(
        verdict
            .violations
            .iter()
            .any(|value| value.contract_id.as_str() == "TXN-001"),
        "{verdict:?}"
    );
}

fn transaction_scenario(
    disposition: TransactionDisposition,
) -> (
    testlab_schema::Scenario,
    OperationId,
    OperationId,
    ProducerId,
) {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.requires.insert(Capability::Transactions);
    let producer_id = id(ProducerId::new("transactional-1"));
    let transaction_id = id(OperationId::new("transaction-1"));
    let operation_id = id(OperationId::new("transaction-record-1"));
    scenario.steps.insert(
        2,
        step(
            "create-transactional",
            ScenarioAction::CreateTransactionalProducer {
                client_id: id(testlab_schema::ClientId::new("client-1")),
                producer_id: producer_id.clone(),
                transactional_id: "fixture-transaction".to_owned(),
                transaction_timeout_ms: 1_000,
                initialization_timeout_ms: 1_000,
                expected_error_code: None,
            },
        ),
    );
    scenario.steps.insert(
        3,
        step(
            "execute-transaction",
            ScenarioAction::ExecuteTransaction {
                producer_id: producer_id.clone(),
                transaction_id: transaction_id.clone(),
                operations: vec![BatchRecord {
                    operation_id: operation_id.clone(),
                    record: record("transaction"),
                }],
                disposition,
                timeout_ms: 1_000,
            },
        ),
    );
    scenario.steps.insert(
        scenario.steps.len() - 1,
        step(
            "close-transactional",
            ScenarioAction::CloseTransactionalProducer(
                testlab_schema::CloseTransactionalProducerAction {
                    producer_id: producer_id.clone(),
                },
            ),
        ),
    );
    scenario.assertions.push(OperationAssertion {
        operation_id: operation_id.clone(),
        accepted: true,
        terminal: Some(TerminalStatus::TransactionStaged),
        visibility: match disposition {
            TransactionDisposition::Commit => VisibilityExpectation::ExactlyOnce,
            TransactionDisposition::Abort => VisibilityExpectation::Absent,
        },
        expected_error_code: None,
    });
    (scenario, operation_id, transaction_id, producer_id)
}

fn transaction_history(
    operation_id: OperationId,
    transaction_id: OperationId,
    producer_id: ProducerId,
    disposition: TransactionDisposition,
) -> Vec<testlab_schema::HistoryEntry> {
    let mut events = history(TerminalStatus::Acknowledged);
    events.extend([
        event(
            10,
            AdapterEvent::TransactionalProducerCreated {
                producer_id: producer_id.clone(),
            },
        ),
        event(
            11,
            AdapterEvent::OperationAccepted {
                operation_id: operation_id.clone(),
            },
        ),
        event(
            12,
            AdapterEvent::OperationTerminal {
                operation_id,
                status: TerminalStatus::TransactionStaged,
                code: None,
                offset: Some(1),
            },
        ),
        event(
            13,
            AdapterEvent::TransactionCompleted {
                transaction_id,
                disposition,
            },
        ),
        event(
            14,
            AdapterEvent::TransactionalProducerClosed { producer_id },
        ),
    ]);
    events
}

fn set_ready_descriptor(
    events: &mut [testlab_schema::HistoryEntry],
    descriptor: testlab_schema::AdapterDescriptor,
) {
    let Some(testlab_schema::HistoryPayload::AdapterEvent { event }) =
        events.first_mut().map(|entry| &mut entry.payload)
    else {
        panic!("fixture ready event missing");
    };
    event.event = AdapterEvent::Ready { descriptor };
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
