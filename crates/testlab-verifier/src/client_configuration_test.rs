//! Tests for the client configuration contract.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, AdapterSecurity,
    ClientConfigurationObservation, ClientId, CommandEnvelope, CommandId, HistoryEntry,
    HistoryPayload, RunId, ScenarioId, TerminalStatus, VisibilityExpectation,
};

use super::verify;
use crate::index::HistoryIndex;

#[test]
fn public_configuration_must_match_the_exact_creation_and_hello() {
    let scenario = crate::verify_fixture::scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    let client_id = ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"));
    let exact = observation(client_id.clone());
    assert!(violations(&scenario, exact.clone()).is_empty());

    let mut wrong_client = exact.clone();
    wrong_client.observed_client_id = None;
    assert_contract(&violations(&scenario, wrong_client));

    let mut wrong_endpoint = exact.clone();
    wrong_endpoint.observed_bootstrap_servers = vec!["127.0.0.1:9093".to_owned()];
    assert_contract(&violations(&scenario, wrong_endpoint));

    let mut wrong_guard = exact;
    wrong_guard.observed_expected_cluster_id = Some("unexpected".to_owned());
    assert_contract(&violations(&scenario, wrong_guard));
}

#[test]
fn observation_must_be_unique_and_follow_its_command() {
    let scenario = crate::verify_fixture::scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    let client_id = ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"));
    let exact = observation(client_id);
    let mut entries = history(exact.clone(), 2);
    entries.push(event(3, "create", exact));
    assert_contract(&verify_history(&scenario, &entries));

    let early = history(
        observation(ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"))),
        0,
    );
    assert_contract(&verify_history(&scenario, &early));
}

fn violations(
    scenario: &testlab_schema::Scenario,
    observation: ClientConfigurationObservation,
) -> Vec<testlab_schema::Violation> {
    verify_history(scenario, &history(observation, 2))
}

fn verify_history(
    scenario: &testlab_schema::Scenario,
    history: &[HistoryEntry],
) -> Vec<testlab_schema::Violation> {
    let mut violations = Vec::new();
    verify(scenario, &HistoryIndex::build(history), &mut violations);
    violations
}

fn history(observation: ClientConfigurationObservation, event_sequence: u64) -> Vec<HistoryEntry> {
    vec![
        command(
            0,
            "hello",
            AdapterCommand::Hello {
                run_id: RunId::new("run-1").unwrap_or_else(|error| panic!("run ID: {error}")),
                scenario_id: ScenarioId::new("producer.verifier")
                    .unwrap_or_else(|error| panic!("scenario ID: {error}")),
                broker_endpoints: vec!["127.0.0.1:9092".to_owned()],
                security: AdapterSecurity::Plaintext,
            },
        ),
        command(
            1,
            "create",
            AdapterCommand::CreateClient(testlab_schema::CreateClientCommand {
                client_id: observation.client_id.clone(),
                expected_cluster_id: None,
            }),
        ),
        event(event_sequence, "create", observation),
    ]
}

fn observation(client_id: ClientId) -> ClientConfigurationObservation {
    ClientConfigurationObservation {
        observed_client_id: Some(client_id.as_str().to_owned()),
        client_id,
        observed_bootstrap_servers: vec!["127.0.0.1:9092".to_owned()],
        observed_expected_cluster_id: None,
        selected_producer_configuration: None,
    }
}

fn command(sequence: u64, id: &str, command: AdapterCommand) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::HarnessCommand {
            command: CommandEnvelope::new(
                CommandId::new(id).unwrap_or_else(|error| panic!("command ID: {error}")),
                command,
            ),
        },
    }
}

fn event(sequence: u64, id: &str, observation: ClientConfigurationObservation) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::AdapterEvent {
            event: AdapterEventEnvelope::new(
                CommandId::new(id).unwrap_or_else(|error| panic!("command ID: {error}")),
                AdapterEvent::ClientCreated(observation),
            ),
        },
    }
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| { violation.contract_id.as_str() == "CLIENT-003" }),
        "{violations:?}"
    );
}
