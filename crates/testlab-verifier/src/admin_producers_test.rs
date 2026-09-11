//! Active-producer verdict tests pin target, field, count, and timing equality.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminProducersDescription, BrokerProducersState,
    BrokerStateObservation, DescribeProducersAction, DescribeProducersCommand, HistoryEntry,
    HistoryPayload, OperationId, ProducerStateSnapshot, ScenarioAction, TerminalStatus,
    VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn exact_nontransactional_and_transactional_producer_states_pass() {
    assert!(violations(history(), 2).is_empty());
}

#[test]
fn count_target_and_field_mismatches_fail() {
    assert_contract(&violations(history(), 1));

    let mut wrong_target = history();
    let HistoryPayload::BrokerStateObservation { observation } = &mut wrong_target[2].payload
    else {
        panic!("independent producer state");
    };
    let BrokerStateObservation::Producers(independent) = observation else {
        panic!("producer observation kind");
    };
    independent.partition = 1;
    assert_contract(&violations(wrong_target, 2));

    let mut wrong_field = history();
    let HistoryPayload::AdapterEvent { event } = &mut wrong_field[1].payload else {
        panic!("public producer event");
    };
    let AdapterEvent::ProducersDescribed(public) = &mut event.event else {
        panic!("public producer description");
    };
    public.producers[0].last_sequence = 9;
    assert_contract(&violations(wrong_field, 2));
}

#[test]
fn noncanonical_public_state_and_delayed_observation_fail() {
    let mut reversed = history();
    let HistoryPayload::AdapterEvent { event } = &mut reversed[1].payload else {
        panic!("public producer event");
    };
    let AdapterEvent::ProducersDescribed(public) = &mut event.event else {
        panic!("public producer description");
    };
    public.producers.reverse();
    assert_contract(&violations(reversed, 2));

    let mut delayed = history();
    delayed.insert(2, command(2, AdapterCommand::Finish));
    delayed[3].sequence = 3;
    delayed[3].observed_unix_ms = 3;
    assert_contract(&violations(delayed, 2));
}

fn history() -> Vec<HistoryEntry> {
    let operation_id = operation();
    let producers = vec![producer(70, Some(17)), producer(71, None)];
    vec![
        command(
            0,
            AdapterCommand::DescribeProducers(DescribeProducersCommand {
                client_id: client(),
                operation_id: operation_id.clone(),
                topic: "orders".to_owned(),
                partition: 0,
                timeout_ms: 1_000,
            }),
        ),
        event(
            1,
            AdapterEvent::ProducersDescribed(AdminProducersDescription {
                operation_id: operation_id.clone(),
                topic: "orders".to_owned(),
                partition: 0,
                producers: producers.clone(),
            }),
        ),
        state(
            2,
            BrokerStateObservation::Producers(BrokerProducersState {
                observation: 4,
                operation_id,
                topic: "orders".to_owned(),
                partition: 0,
                producers,
            }),
        ),
    ]
}

fn violations(history: Vec<HistoryEntry>, expected_count: usize) -> Vec<testlab_schema::Violation> {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.steps.insert(
        2,
        step(
            "describe-producers",
            ScenarioAction::DescribeProducers(DescribeProducersAction {
                client_id: client(),
                operation_id: operation(),
                topic: "orders".to_owned(),
                partition: 0,
                expected_producer_count: expected_count,
                timeout_ms: 1_000,
            }),
        ),
    );
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    verify_admin(&scenario, &index, &[], &mut violations);
    violations
}

fn state(sequence: u64, observation: BrokerStateObservation) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation { observation },
    }
}

fn producer(producer_id: i64, start: Option<i64>) -> ProducerStateSnapshot {
    ProducerStateSnapshot {
        producer_id,
        producer_epoch: 2,
        last_sequence: 0,
        last_timestamp: 1_700_000_000_000 + producer_id,
        coordinator_epoch: 4,
        current_transaction_start_offset: start,
    }
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-describe-producers-1")
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-051"),
        "{violations:?}"
    );
}
