//! Successful client creation proves exact public client and builder configuration readback.

use testlab_schema::{
    AdapterCommand, AdapterEvent, ClientConfigurationObservation, ClientId, ProducerConfiguration,
    Scenario, ScenarioAction, Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;

struct ExpectedCreation {
    command: AdapterCommand,
    client_id: ClientId,
    expected_cluster_id: Option<String>,
    producer_configuration: Option<ProducerConfiguration>,
}

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    if index.commands.is_empty() {
        return;
    }
    let Some(endpoints) = index
        .commands
        .iter()
        .find_map(|(_, _, command)| match command {
            AdapterCommand::Hello {
                broker_endpoints, ..
            } => Some(broker_endpoints.as_slice()),
            _ => None,
        })
    else {
        return;
    };
    let mut command_cursor = 0;
    for expected in successful_creations(scenario) {
        let Some(relative) = index.commands[command_cursor..]
            .iter()
            .position(|(_, _, command)| command == &expected.command)
        else {
            continue;
        };
        command_cursor += relative;
        let (command_sequence, command_id, _) = &index.commands[command_cursor];
        command_cursor += 1;
        let events = index
            .adapter_events
            .iter()
            .filter(|(_, envelope)| &envelope.command_id == command_id)
            .filter_map(|(sequence, envelope)| match &envelope.event {
                AdapterEvent::ClientCreated(observation) => Some((*sequence, observation)),
                _ => None,
            })
            .collect::<Vec<_>>();
        let exact = events.len() == 1
            && events.first().is_some_and(|(event_sequence, observation)| {
                *event_sequence > *command_sequence
                    && observation.client_id == expected.client_id
                    && observation.observed_client_id.as_deref()
                        == Some(expected.client_id.as_str())
                    && observation.observed_bootstrap_servers == endpoints
                    && observation.observed_expected_cluster_id == expected.expected_cluster_id
            });
        if !exact {
            let mut evidence = vec![format!("history:{command_sequence}")];
            evidence.extend(
                events
                    .iter()
                    .map(|(sequence, _)| format!("history:{sequence}")),
            );
            violations.push(violation(
                "CLIENT-003",
                format!(
                    "client {} expected one later exact public configuration observation",
                    expected.client_id
                ),
                None,
                evidence,
            ));
        }
        verify_producer_configuration(&expected, *command_sequence, &events, violations);
    }
}

fn verify_producer_configuration(
    expected: &ExpectedCreation,
    command_sequence: u64,
    events: &[(u64, &ClientConfigurationObservation)],
    violations: &mut Vec<Violation>,
) {
    let exact = events.len() == 1
        && events.first().is_some_and(|(event_sequence, observation)| {
            *event_sequence > command_sequence
                && observation.selected_producer_configuration.as_ref()
                    == expected.producer_configuration.as_ref()
        });
    if !exact {
        violations.push(violation(
            "PROD-024",
            format!(
                "client {} expected one later exact selected producer-policy observation",
                expected.client_id
            ),
            None,
            std::iter::once(command_sequence)
                .chain(events.iter().map(|(sequence, _)| *sequence))
                .map(|sequence| format!("history:{sequence}"))
                .collect(),
        ));
    }
}

fn successful_creations(scenario: &Scenario) -> Vec<ExpectedCreation> {
    scenario
        .steps
        .iter()
        .filter_map(|step| match &step.action {
            ScenarioAction::CreateClient(action) if action.expected_error_code.is_none() => {
                Some(ExpectedCreation {
                    command: AdapterCommand::CreateClient(testlab_schema::CreateClientCommand {
                        client_id: action.client_id.clone(),
                        expected_cluster_id: action.expected_cluster_id.clone(),
                    }),
                    client_id: action.client_id.clone(),
                    expected_cluster_id: action.expected_cluster_id.clone(),
                    producer_configuration: None,
                })
            }
            ScenarioAction::CreateConfiguredClient(action) => Some(ExpectedCreation {
                command: AdapterCommand::CreateConfiguredClient(action.clone()),
                client_id: action.client_id.clone(),
                expected_cluster_id: None,
                producer_configuration: Some(action.configuration),
            }),
            ScenarioAction::CreateAssignedConsumerClient(action) => Some(ExpectedCreation {
                command: AdapterCommand::CreateAssignedConsumerClient(action.clone()),
                client_id: action.client_id.clone(),
                expected_cluster_id: None,
                producer_configuration: None,
            }),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
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
        let client_id =
            ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"));
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
        let client_id =
            ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"));
        let exact = observation(client_id);
        let mut entries = history(exact.clone(), 2);
        entries.push(event(3, "create", exact));
        assert_contract(&verify_history(&scenario, &entries));

        let early = history(
            observation(
                ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}")),
            ),
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

    fn history(
        observation: ClientConfigurationObservation,
        event_sequence: u64,
    ) -> Vec<HistoryEntry> {
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
}
