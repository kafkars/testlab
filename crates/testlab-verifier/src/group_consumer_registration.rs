//! Every hosted group member requires one exact public registration command.

use testlab_schema::{AdapterCommand, ConsumerId, Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    if index.commands.is_empty() {
        return;
    }
    for step in &scenario.steps {
        let ScenarioAction::CreateGroupConsumer { consumer_id, .. } = &step.action else {
            continue;
        };
        let relevant = index
            .commands
            .iter()
            .filter(|(_, _, command)| creation_consumer_id(command) == Some(consumer_id))
            .collect::<Vec<_>>();
        let expected = command(&step.action);
        let exact =
            relevant.len() == 1 && relevant.iter().all(|(_, _, actual)| *actual == expected);
        if exact {
            continue;
        }
        violations.push(violation(
            "CONS-028",
            format!(
                "group consumer {consumer_id} expected one exact client, group, ordered subscription, protocol, and policy registration command"
            ),
            None,
            relevant
                .into_iter()
                .map(|(sequence, _, _)| format!("history:{sequence}"))
                .collect(),
        ));
    }
}

fn command(action: &ScenarioAction) -> AdapterCommand {
    let ScenarioAction::CreateGroupConsumer {
        client_id,
        consumer_id,
        group_id,
        topics,
        protocol,
        configuration,
    } = action
    else {
        unreachable!("group registration command requires group creation");
    };
    AdapterCommand::CreateGroupConsumer {
        client_id: client_id.clone(),
        consumer_id: consumer_id.clone(),
        group_id: group_id.clone(),
        topics: topics.clone(),
        protocol: *protocol,
        configuration: configuration.clone(),
    }
}

fn creation_consumer_id(command: &AdapterCommand) -> Option<&ConsumerId> {
    match command {
        AdapterCommand::CreateAssignedConsumer { consumer_id, .. }
        | AdapterCommand::CreateGroupConsumer { consumer_id, .. }
        | AdapterCommand::CreateShareConsumer { consumer_id, .. } => Some(consumer_id),
        _ => None,
    }
}

#[cfg(test)]
#[path = "group_consumer_registration_test.rs"]
mod tests;
