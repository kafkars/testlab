//! Admin partition-abort verdict tests pin identity, transition, and timing.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminProducersDescription, BatchRecord, BrokerProducersState,
    BrokerStateObservation, HistoryEntry, HistoryPayload, OperationId, ProducerId,
    ProducerStateSnapshot, ScenarioAction, TransactionDisposition,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, record};

#[test]
fn exact_open_to_cleared_transition_passes() {
    assert!(violations(history()).is_empty());
}

#[test]
fn changed_public_state_or_inexact_command_fails() {
    let mut changed = history();
    let HistoryPayload::AdapterEvent { event } = &mut changed[2].payload else {
        panic!("post-abort public state");
    };
    let AdapterEvent::ProducersDescribed(state) = &mut event.event else {
        panic!("producer state event");
    };
    state.producers[0].last_sequence += 1;
    assert_contract(&violations(changed));

    let mut inexact = history();
    let HistoryPayload::HarnessCommand { command } = &mut inexact[0].payload else {
        panic!("transaction command");
    };
    let AdapterCommand::ExecuteTransaction { timeout_ms, .. } = &mut command.command else {
        panic!("execute transaction command");
    };
    *timeout_ms += 1;
    assert_contract(&violations(inexact));

    let mut independent_mismatch = history();
    let HistoryPayload::BrokerStateObservation { observation } =
        &mut independent_mismatch[4].payload
    else {
        panic!("independent producer state");
    };
    let BrokerStateObservation::Producers(state) = observation else {
        panic!("producer state observation");
    };
    state.producers[0].producer_id += 1;
    assert_contract(&violations(independent_mismatch));
}

#[test]
fn cleanup_before_public_proof_or_delayed_cli_state_fails() {
    let mut early_completion = history();
    early_completion.swap(2, 3);
    early_completion[2].sequence = 2;
    early_completion[3].sequence = 3;
    assert_contract(&violations(early_completion));

    let mut delayed = history();
    delayed.insert(4, command(4, AdapterCommand::Finish));
    delayed[5].sequence = 5;
    delayed[5].observed_unix_ms = 5;
    assert_contract(&violations(delayed));
}

fn history() -> Vec<HistoryEntry> {
    let action = action();
    let ScenarioAction::ExecuteTransaction {
        transaction_id,
        operations,
        ..
    } = &action
    else {
        unreachable!();
    };
    let operation_id = operations[0].operation_id.clone();
    let mut cleared = producer(Some(17));
    let open = cleared.clone();
    cleared.current_transaction_start_offset = None;
    cleared.last_timestamp += 1;
    let mut independently_cleared = cleared.clone();
    independently_cleared.last_timestamp += 1;
    vec![
        command(0, exact_command(&action)),
        event(
            1,
            AdapterEvent::ProducersDescribed(description(operation_id, open)),
        ),
        event(
            2,
            AdapterEvent::ProducersDescribed(description(transaction_id.clone(), cleared.clone())),
        ),
        event(
            3,
            AdapterEvent::TransactionCompleted {
                transaction_id: transaction_id.clone(),
                disposition: TransactionDisposition::AdminPartitionAbort,
            },
        ),
        state(
            4,
            BrokerStateObservation::Producers(BrokerProducersState {
                observation: 7,
                operation_id: transaction_id.clone(),
                topic: "records".to_owned(),
                partition: 0,
                producers: vec![independently_cleared],
            }),
        ),
    ]
}

fn action() -> ScenarioAction {
    ScenarioAction::ExecuteTransaction {
        producer_id: ProducerId::new("transactional-1")
            .unwrap_or_else(|error| panic!("producer ID: {error}")),
        transaction_id: id("admin-abort-1"),
        operations: vec![BatchRecord {
            operation_id: id("admin-abort-record-1"),
            record: record("aborted"),
        }],
        method: Default::default(),
        disposition: TransactionDisposition::AdminPartitionAbort,
        timeout_ms: 1_000,
    }
}

fn exact_command(action: &ScenarioAction) -> AdapterCommand {
    let ScenarioAction::ExecuteTransaction {
        producer_id,
        transaction_id,
        operations,
        method,
        disposition,
        timeout_ms,
    } = action
    else {
        unreachable!();
    };
    AdapterCommand::ExecuteTransaction {
        producer_id: producer_id.clone(),
        transaction_id: transaction_id.clone(),
        operations: operations.clone(),
        method: *method,
        disposition: *disposition,
        timeout_ms: *timeout_ms,
    }
}

fn description(
    operation_id: OperationId,
    producer: ProducerStateSnapshot,
) -> AdminProducersDescription {
    AdminProducersDescription {
        operation_id,
        topic: "records".to_owned(),
        partition: 0,
        producers: vec![producer],
    }
}

fn producer(start: Option<i64>) -> ProducerStateSnapshot {
    ProducerStateSnapshot {
        producer_id: 71,
        producer_epoch: 2,
        last_sequence: 0,
        last_timestamp: 1_700_000_000_000,
        coordinator_epoch: 4,
        current_transaction_start_offset: start,
    }
}

fn state(sequence: u64, observation: BrokerStateObservation) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation { observation },
    }
}

fn violations(history: Vec<HistoryEntry>) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    super::transaction_admin_abort::verify(&action(), &index, &mut violations);
    violations
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-075"),
        "{violations:?}"
    );
}

fn id(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}
