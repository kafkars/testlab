//! Producer-fencing verdicts pin caller order, returned identities, and CLI timing.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminProducersFenced, BrokerStateObservation,
    BrokerTransactionState, FenceProducersAction, FenceProducersCommand, FencedProducerSnapshot,
    HistoryEntry, HistoryPayload, OperationId, ProducerFenceExpectation, ScenarioAction,
    TerminalStatus, TransactionDescriptionSnapshot, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn exact_fencing_and_cli_snapshots_pass() {
    assert!(violations(history()).is_empty());
}

#[test]
fn fencing_identity_order_and_timing_mismatches_fail() {
    let mut wrong_epoch = history();
    fenced_event(&mut wrong_epoch).producers[0].producer_epoch = 8;
    assert_contract(&violations(wrong_epoch));

    let mut reordered = history();
    fenced_event(&mut reordered).producers.reverse();
    assert_contract(&violations(reordered));

    let mut noncontiguous = history();
    broker_state(&mut noncontiguous, 1).observation = 12;
    assert_contract(&violations(noncontiguous));

    let mut over_deadline = history();
    broker_state(&mut over_deadline, 0)
        .transaction
        .transaction_timeout_ms = 20_001;
    assert_contract(&violations(over_deadline));
}

fn history() -> Vec<HistoryEntry> {
    let producers = fenced_producers();
    vec![
        command(
            0,
            AdapterCommand::FenceProducers(FenceProducersCommand {
                client_id: client(),
                operation_id: operation(),
                transactional_ids: ids().map(str::to_owned).collect(),
                timeout_ms: 20_000,
            }),
        ),
        event(
            1,
            AdapterEvent::ProducersFenced(AdminProducersFenced {
                operation_id: operation(),
                throttle_time_ms: 3,
                producers: producers.clone(),
            }),
        ),
        state(2, broker(7, &producers[0])),
        state(3, broker(8, &producers[1])),
    ]
}

fn violations(history: Vec<HistoryEntry>) -> Vec<testlab_schema::Violation> {
    let action = ScenarioAction::FenceProducers(FenceProducersAction {
        client_id: client(),
        operation_id: operation(),
        producers: ids().map(expectation).collect(),
        timeout_ms: 20_000,
    });
    let mut fixture = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    fixture.steps.insert(2, step("producer-fencing", action));
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    verify_admin(&fixture, &index, &[], &mut violations);
    violations
}

fn fenced_producers() -> Vec<FencedProducerSnapshot> {
    vec![fenced(zulu_id(), 9, 2), fenced(alpha_id(), 4, 3)]
}

fn fenced(transactional_id: &str, producer_id: i64, producer_epoch: i16) -> FencedProducerSnapshot {
    FencedProducerSnapshot {
        transactional_id: transactional_id.to_owned(),
        producer_id,
        producer_epoch,
    }
}

fn broker(observation: u64, producer: &FencedProducerSnapshot) -> BrokerStateObservation {
    BrokerStateObservation::Transaction(BrokerTransactionState {
        observation,
        operation_id: operation(),
        coordinator_id: 1,
        transaction: TransactionDescriptionSnapshot {
            transactional_id: producer.transactional_id.clone(),
            transaction_state: "Empty".to_owned(),
            transaction_timeout_ms: 19_000,
            transaction_start_time_ms: None,
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            topics: Vec::new(),
        },
    })
}

fn expectation(transactional_id: &str) -> ProducerFenceExpectation {
    ProducerFenceExpectation {
        transactional_id: transactional_id.to_owned(),
        expected_state: "Empty".to_owned(),
        expected_start_time_present: false,
        expected_topics: Vec::new(),
    }
}

fn state(sequence: u64, observation: BrokerStateObservation) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation { observation },
    }
}

fn fenced_event(entries: &mut [HistoryEntry]) -> &mut AdminProducersFenced {
    let HistoryPayload::AdapterEvent { event } = &mut entries[1].payload else {
        panic!("producer fencing event");
    };
    let AdapterEvent::ProducersFenced(value) = &mut event.event else {
        panic!("producer fencing payload");
    };
    value
}

fn broker_state(entries: &mut [HistoryEntry], index: usize) -> &mut BrokerTransactionState {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[2 + index].payload
    else {
        panic!("producer fencing observation");
    };
    let BrokerStateObservation::Transaction(value) = observation else {
        panic!("producer fencing state");
    };
    value
}

fn ids() -> impl Iterator<Item = &'static str> {
    [zulu_id(), alpha_id()].into_iter()
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-fence-producers").unwrap_or_else(|error| panic!("operation: {error}"))
}

fn alpha_id() -> &'static str {
    "testlab-admin-fence-alpha"
}

fn zulu_id() -> &'static str {
    "testlab-admin-fence-zulu"
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-057"),
        "{violations:?}"
    );
}
