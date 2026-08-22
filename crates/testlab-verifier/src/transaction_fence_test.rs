//! Transaction fencing tests bind the public error to independent read-committed absence.

use testlab_schema::{
    AdapterDescriptor, AdapterEvent, BatchRecord, Capability, ClientId, OperationAssertion,
    OperationId, ProducerId, ScenarioAction, TerminalStatus, VisibilityExpectation,
};

use super::verify;
use crate::verify_fixture::{adapter, event, history, observation, record, scenario, step};

#[test]
fn fenced_commit_and_absent_record_pass() {
    let (scenario, events, descriptor, _) = fence_fixture(Some("fenced"));
    let observations = [observation(0, "value")];

    let verdict = verify(&scenario, &descriptor, &events, &observations);

    assert!(verdict.is_passed(), "{verdict:?}");
}

#[test]
fn wrong_fence_code_and_visible_record_fail() {
    let (scenario, events, descriptor, operation_id) = fence_fixture(Some("timeout"));
    let mut leaked = observation(1, "fenced");
    leaked.operation_id = operation_id;
    leaked.record = record("fenced");
    leaked.digest = leaked
        .record
        .digest()
        .unwrap_or_else(|error| panic!("record digest: {error}"));

    let observations = [observation(0, "value"), leaked];
    let verdict = verify(&scenario, &descriptor, &events, &observations);

    assert!(
        verdict
            .violations
            .iter()
            .filter(|value| value.contract_id.as_str() == "TXN-003")
            .count()
            >= 2
    );
}

fn fence_fixture(
    commit_error_code: Option<&str>,
) -> (
    testlab_schema::Scenario,
    Vec<testlab_schema::HistoryEntry>,
    AdapterDescriptor,
    OperationId,
) {
    let ids = FenceIds {
        client: id(ClientId::new("client-1")),
        original: id(ProducerId::new("transactional-original")),
        replacement: id(ProducerId::new("transactional-replacement")),
        transaction: id(OperationId::new("transaction-fenced")),
        operation: id(OperationId::new("operation-fenced")),
    };
    let scenario = fence_scenario(&ids);
    let mut descriptor = adapter();
    descriptor.capabilities.insert(Capability::Transactions);
    let events = fence_history(&ids, commit_error_code, descriptor.clone());
    (scenario, events, descriptor, ids.operation)
}

struct FenceIds {
    client: ClientId,
    original: ProducerId,
    replacement: ProducerId,
    transaction: OperationId,
    operation: OperationId,
}

fn fence_scenario(ids: &FenceIds) -> testlab_schema::Scenario {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.requires.insert(Capability::Transactions);
    scenario.steps.insert(
        3,
        step(
            "create-transactional-original",
            ScenarioAction::CreateTransactionalProducer {
                client_id: ids.client.clone(),
                producer_id: ids.original.clone(),
                transactional_id: "fixture-fenced-owner".to_owned(),
                transaction_timeout_ms: 1_000,
                initialization_timeout_ms: 1_000,
            },
        ),
    );
    scenario.steps.insert(
        4,
        step(
            "fence-transaction",
            ScenarioAction::FenceTransaction {
                producer_id: ids.original.clone(),
                transaction_id: ids.transaction.clone(),
                operation: BatchRecord {
                    operation_id: ids.operation.clone(),
                    record: record("fenced"),
                },
                replacement_client_id: ids.client.clone(),
                replacement_producer_id: ids.replacement.clone(),
                transactional_id: "fixture-fenced-owner".to_owned(),
                transaction_timeout_ms: 1_000,
                initialization_timeout_ms: 1_000,
                timeout_ms: 1_000,
            },
        ),
    );
    let shutdown = scenario.steps.len() - 1;
    scenario.steps.insert(
        shutdown,
        step(
            "close-transactional-original",
            ScenarioAction::CloseTransactionalProducer {
                producer_id: ids.original.clone(),
            },
        ),
    );
    scenario.steps.insert(
        shutdown + 1,
        step(
            "close-transactional-replacement",
            ScenarioAction::CloseTransactionalProducer {
                producer_id: ids.replacement.clone(),
            },
        ),
    );
    scenario.assertions.push(OperationAssertion {
        operation_id: ids.operation.clone(),
        accepted: true,
        terminal: Some(TerminalStatus::TransactionStaged),
        visibility: VisibilityExpectation::Absent,
    });
    scenario
}

fn fence_history(
    ids: &FenceIds,
    commit_error_code: Option<&str>,
    descriptor: AdapterDescriptor,
) -> Vec<testlab_schema::HistoryEntry> {
    let mut events = history(TerminalStatus::Acknowledged);
    set_ready_descriptor(&mut events, descriptor);
    events.extend([
        event(
            10,
            AdapterEvent::TransactionalProducerCreated {
                producer_id: ids.original.clone(),
            },
        ),
        event(
            11,
            AdapterEvent::OperationAccepted {
                operation_id: ids.operation.clone(),
            },
        ),
        event(
            12,
            AdapterEvent::OperationTerminal {
                operation_id: ids.operation.clone(),
                status: TerminalStatus::TransactionStaged,
                code: None,
                offset: Some(1),
            },
        ),
        event(
            13,
            AdapterEvent::TransactionalProducerCreated {
                producer_id: ids.replacement.clone(),
            },
        ),
        event(
            14,
            AdapterEvent::TransactionFenceCompleted {
                transaction_id: ids.transaction.clone(),
                commit_error_code: commit_error_code.map(str::to_owned),
            },
        ),
        event(
            15,
            AdapterEvent::TransactionalProducerClosed {
                producer_id: ids.original.clone(),
            },
        ),
        event(
            16,
            AdapterEvent::TransactionalProducerClosed {
                producer_id: ids.replacement.clone(),
            },
        ),
    ]);
    events
}

fn set_ready_descriptor(
    events: &mut [testlab_schema::HistoryEntry],
    descriptor: AdapterDescriptor,
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
