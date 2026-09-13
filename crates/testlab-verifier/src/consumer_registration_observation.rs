//! Hosted consumer registration proves selected builder policy and returned-handle identity.

use testlab_schema::{
    AdapterCommand, AdapterEvent, CommandId, ConsumerId, GroupConsumerConfigurationSelection,
    GroupProtocol, Scenario, ScenarioAction, Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;

enum ExpectedRegistration {
    Group {
        command: AdapterCommand,
        consumer_id: ConsumerId,
        group_id: String,
        subscription: Vec<String>,
        selected_protocol: GroupProtocol,
        selected_configuration: Option<GroupConsumerConfigurationSelection>,
    },
    Share {
        command: AdapterCommand,
        consumer_id: ConsumerId,
        group_id: String,
        subscription: Vec<String>,
        rack: Option<String>,
    },
}

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    if index.commands.is_empty() {
        return;
    }
    let mut command_cursor = 0;
    for expected in registrations(scenario) {
        let Some(relative) = index.commands[command_cursor..]
            .iter()
            .position(|(_, _, command)| command == expected.command())
        else {
            continue;
        };
        command_cursor += relative;
        let (command_sequence, command_id, _) = &index.commands[command_cursor];
        command_cursor += 1;
        verify_registration(&expected, *command_sequence, command_id, index, violations);
    }
}

fn verify_registration(
    expected: &ExpectedRegistration,
    command_sequence: u64,
    command_id: &CommandId,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let events = index
        .adapter_events
        .iter()
        .filter(|(_, envelope)| &envelope.command_id == command_id)
        .filter_map(|(sequence, envelope)| {
            matches!(
                &envelope.event,
                AdapterEvent::GroupConsumerCreated(_) | AdapterEvent::ShareConsumerCreated(_)
            )
            .then_some((*sequence, &envelope.event))
        })
        .collect::<Vec<_>>();
    let exact = events.len() == 1
        && events.first().is_some_and(|(sequence, event)| {
            *sequence > command_sequence && expected.matches(event)
        });
    if !exact {
        let mut evidence = vec![format!("history:{command_sequence}")];
        evidence.extend(
            events
                .iter()
                .map(|(sequence, _)| format!("history:{sequence}")),
        );
        violations.push(violation(
            expected.contract_id(),
            format!(
                "hosted consumer {} expected one later exact public registration observation",
                expected.consumer_id()
            ),
            None,
            evidence,
        ));
    }
    verify_group_configuration(expected, command_sequence, &events, violations);
}

fn verify_group_configuration(
    expected: &ExpectedRegistration,
    command_sequence: u64,
    events: &[(u64, &AdapterEvent)],
    violations: &mut Vec<Violation>,
) {
    let ExpectedRegistration::Group {
        consumer_id,
        selected_protocol,
        selected_configuration,
        ..
    } = expected
    else {
        return;
    };
    let exact = events.len() == 1
        && events.first().is_some_and(|(sequence, event)| {
            *sequence > command_sequence
                && matches!(
                    event,
                    AdapterEvent::GroupConsumerCreated(observation)
                        if observation.selected_protocol == *selected_protocol
                            && observation.selected_configuration.as_ref()
                                == selected_configuration.as_ref()
                )
        });
    if !exact {
        violations.push(violation(
            "CONS-035",
            format!(
                "group consumer {consumer_id} expected one later exact selected builder-policy observation"
            ),
            None,
            std::iter::once(command_sequence)
                .chain(events.iter().map(|(sequence, _)| *sequence))
                .map(|sequence| format!("history:{sequence}"))
                .collect(),
        ));
    }
}

impl ExpectedRegistration {
    fn command(&self) -> &AdapterCommand {
        match self {
            Self::Group { command, .. } | Self::Share { command, .. } => command,
        }
    }

    fn consumer_id(&self) -> &ConsumerId {
        match self {
            Self::Group { consumer_id, .. } | Self::Share { consumer_id, .. } => consumer_id,
        }
    }

    const fn contract_id(&self) -> &'static str {
        match self {
            Self::Group { .. } => "CONS-034",
            Self::Share { .. } => "SHARE-015",
        }
    }

    fn matches(&self, event: &AdapterEvent) -> bool {
        match (self, event) {
            (
                Self::Group {
                    consumer_id,
                    group_id,
                    subscription,
                    ..
                },
                AdapterEvent::GroupConsumerCreated(observation),
            ) => {
                observation.consumer_id == *consumer_id
                    && observation.group_id == *group_id
                    && observation.subscription == *subscription
            }
            (
                Self::Share {
                    consumer_id,
                    group_id,
                    subscription,
                    rack,
                    ..
                },
                AdapterEvent::ShareConsumerCreated(observation),
            ) => {
                observation.consumer_id == *consumer_id
                    && observation.group_id == *group_id
                    && observation.subscription == *subscription
                    && observation.rack == *rack
            }
            _ => false,
        }
    }
}

fn registrations(scenario: &Scenario) -> Vec<ExpectedRegistration> {
    scenario
        .steps
        .iter()
        .filter_map(|step| registration(&step.action))
        .collect()
}

fn registration(action: &ScenarioAction) -> Option<ExpectedRegistration> {
    match action {
        ScenarioAction::CreateGroupConsumer {
            client_id,
            consumer_id,
            group_id,
            topics,
            protocol,
            configuration,
        } => Some(ExpectedRegistration::Group {
            command: AdapterCommand::CreateGroupConsumer {
                client_id: client_id.clone(),
                consumer_id: consumer_id.clone(),
                group_id: group_id.clone(),
                topics: topics.clone(),
                protocol: *protocol,
                configuration: configuration.clone(),
            },
            consumer_id: consumer_id.clone(),
            group_id: group_id.clone(),
            subscription: topics.clone(),
            selected_protocol: *protocol,
            selected_configuration: configuration
                .as_ref()
                .map(GroupConsumerConfigurationSelection::from),
        }),
        ScenarioAction::CreateShareConsumer {
            client_id,
            consumer_id,
            group_id,
            topics,
            rack,
            membership_timeout_ms,
            close_timeout_ms,
            configuration,
        } => Some(ExpectedRegistration::Share {
            command: AdapterCommand::CreateShareConsumer {
                client_id: client_id.clone(),
                consumer_id: consumer_id.clone(),
                group_id: group_id.clone(),
                topics: topics.clone(),
                rack: rack.clone(),
                membership_timeout_ms: *membership_timeout_ms,
                close_timeout_ms: *close_timeout_ms,
                configuration: *configuration,
            },
            consumer_id: consumer_id.clone(),
            group_id: group_id.clone(),
            subscription: topics.clone(),
            rack: rack.clone(),
        }),
        _ => None,
    }
}

#[cfg(test)]
#[path = "consumer_registration_observation_test.rs"]
mod tests;
