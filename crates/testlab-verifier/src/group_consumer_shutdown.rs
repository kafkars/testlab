//! Group-consumer shutdown verification binds public request counts to terminal observation.

use testlab_schema::{
    AdapterCommand, AdapterEvent, GroupConsumerShutdownCommand, Scenario, ScenarioAction, Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for step in &scenario.steps {
        let ScenarioAction::ShutdownGroupConsumer(action) = &step.action else {
            continue;
        };
        let expected = GroupConsumerShutdownCommand {
            operation_id: action.operation_id.clone(),
            consumer_id: action.consumer_id.clone(),
            request_count: action.request_count,
            timeout_ms: action.timeout_ms,
        };
        let commands = index
            .commands
            .iter()
            .filter(|(_, _, command)| {
                matches!(command, AdapterCommand::ShutdownGroupConsumer(actual) if actual.operation_id == action.operation_id)
            })
            .collect::<Vec<_>>();
        let completions = index
            .adapter_events
            .iter()
            .filter(|(_, envelope)| {
                matches!(&envelope.event, AdapterEvent::GroupConsumerShutdownCompleted(actual) if actual.operation_id == action.operation_id)
            })
            .collect::<Vec<_>>();
        let exact = commands.len() == 1
            && completions.len() == 1
            && matches!(&commands[0].2, AdapterCommand::ShutdownGroupConsumer(actual) if actual == &expected)
            && completion_matches(commands[0], completions[0], action);
        if !exact {
            violations.push(violation(
                "LIFE-015",
                format!(
                    "group shutdown {} expected one exact command and correlated public stream termination after {} request(s)",
                    action.operation_id, action.request_count
                ),
                Some(action.operation_id.clone()),
                completions
                    .iter()
                    .map(|(sequence, _)| format!("history:{sequence}"))
                    .collect(),
            ));
        }
    }
}

fn completion_matches(
    command: &(u64, testlab_schema::CommandId, AdapterCommand),
    completion: &(u64, testlab_schema::AdapterEventEnvelope),
    action: &testlab_schema::GroupConsumerShutdownAction,
) -> bool {
    let AdapterEvent::GroupConsumerShutdownCompleted(actual) = &completion.1.event else {
        return false;
    };
    completion.0 > command.0
        && completion.1.command_id == command.1
        && actual.operation_id == action.operation_id
        && actual.consumer_id == action.consumer_id
        && actual.request_count == action.request_count
}
