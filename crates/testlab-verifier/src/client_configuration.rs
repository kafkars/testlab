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
#[path = "client_configuration_test.rs"]
mod tests;
