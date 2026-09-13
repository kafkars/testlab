//! Producer handle observation tests pin explicit, inherited, and default timeout selection.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, CommandEnvelope, CommandId, HistoryEntry,
    HistoryPayload, ProducerHandleConfigurationObservation, ProducerId, Scenario, ScenarioAction,
    TerminalStatus, VisibilityExpectation,
};

use super::verify;
use crate::index::HistoryIndex;

#[test]
fn explicit_and_inherited_timeouts_require_exact_readback() {
    let explicit = parse(include_str!(
        "../../../scenarios/kafka/producer-round-trip.toml"
    ));
    assert!(violations(&explicit, exact_history(&explicit, 15_000)).is_empty());

    let inherited = parse(include_str!(
        "../../../scenarios/kafka/producer-configuration-gzip.toml"
    ));
    assert!(violations(&inherited, exact_history(&inherited, 20_000)).is_empty());

    assert_contract(&violations(&inherited, exact_history(&inherited, 20_001)));
    let mut wrong_producer = exact_history(&explicit, 15_000);
    observation_mut(&mut wrong_producer[1]).producer_id = producer("other");
    assert_contract(&violations(&explicit, wrong_producer));
}

#[test]
fn default_selection_is_nonzero_and_observation_is_correlated_unique_and_later() {
    let scenario = crate::verify_fixture::scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    assert!(violations(&scenario, exact_history(&scenario, 30_000)).is_empty());
    assert_contract(&violations(&scenario, exact_history(&scenario, 0)));

    let mut duplicate = exact_history(&scenario, 30_000);
    duplicate.push(event(
        3,
        "producer",
        observation(producer_id(&producer_command(&scenario)), 30_000),
    ));
    assert_contract(&violations(&scenario, duplicate));

    let mut early = exact_history(&scenario, 30_000);
    early[1].sequence = 0;
    assert_contract(&violations(&scenario, early));

    let mut wrong_correlation = exact_history(&scenario, 30_000);
    let HistoryPayload::AdapterEvent { event } = &mut wrong_correlation[1].payload else {
        panic!("adapter event");
    };
    event.command_id = command_id("other");
    assert_contract(&violations(&scenario, wrong_correlation));
}

fn exact_history(scenario: &Scenario, selected_delivery_timeout_ms: u64) -> Vec<HistoryEntry> {
    let command = producer_command(scenario);
    let producer_id = producer_id(&command);
    vec![
        history_command(1, "producer", command),
        event(
            2,
            "producer",
            observation(producer_id, selected_delivery_timeout_ms),
        ),
    ]
}

fn producer_command(scenario: &Scenario) -> AdapterCommand {
    scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::CreateProducer {
                client_id,
                producer_id,
                ownership,
                delivery_timeout_ms,
            } => Some(AdapterCommand::CreateProducer {
                client_id: client_id.clone(),
                producer_id: producer_id.clone(),
                ownership: *ownership,
                delivery_timeout_ms: *delivery_timeout_ms,
            }),
            _ => None,
        })
        .unwrap_or_else(|| panic!("producer creation"))
}

fn producer_id(command: &AdapterCommand) -> ProducerId {
    let AdapterCommand::CreateProducer { producer_id, .. } = command else {
        panic!("producer creation command");
    };
    producer_id.clone()
}

fn observation(producer_id: ProducerId, selected_delivery_timeout_ms: u64) -> AdapterEvent {
    AdapterEvent::ProducerCreated(ProducerHandleConfigurationObservation {
        producer_id,
        selected_delivery_timeout_ms,
    })
}

fn observation_mut(entry: &mut HistoryEntry) -> &mut ProducerHandleConfigurationObservation {
    let HistoryPayload::AdapterEvent { event } = &mut entry.payload else {
        panic!("adapter event");
    };
    let AdapterEvent::ProducerCreated(observation) = &mut event.event else {
        panic!("producer creation event");
    };
    observation
}

fn history_command(sequence: u64, id: &str, command: AdapterCommand) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::HarnessCommand {
            command: CommandEnvelope::new(command_id(id), command),
        },
    }
}

fn event(sequence: u64, id: &str, event: AdapterEvent) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::AdapterEvent {
            event: AdapterEventEnvelope::new(command_id(id), event),
        },
    }
}

fn parse(value: &str) -> Scenario {
    toml::from_str(value).unwrap_or_else(|error| panic!("parse scenario: {error}"))
}

fn producer(value: &str) -> ProducerId {
    ProducerId::new(value).unwrap_or_else(|error| panic!("producer ID: {error}"))
}

fn command_id(value: &str) -> CommandId {
    CommandId::new(value).unwrap_or_else(|error| panic!("command ID: {error}"))
}

fn violations(scenario: &Scenario, history: Vec<HistoryEntry>) -> Vec<testlab_schema::Violation> {
    let mut violations = Vec::new();
    verify(scenario, &HistoryIndex::build(&history), &mut violations);
    violations
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "PROD-023"),
        "{violations:?}"
    );
}
