//! Aggregate hosted-group operation policy retains its exact public selection.

#[cfg(test)]
#[path = "group_operation_config_test.rs"]
mod tests;

use testlab_schema::{
    AdapterCommand, GroupOperationConfigMethod, Scenario, ScenarioAction, Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for step in &scenario.steps {
        let ScenarioAction::CreateGroupConsumer {
            consumer_id,
            configuration: Some(configuration),
            ..
        } = &step.action
        else {
            continue;
        };
        if configuration.operation_config_method != GroupOperationConfigMethod::OperationConfig
            || !index.action_issued(&step.action)
        {
            continue;
        }
        let expected = command(&step.action);
        let commands = index
            .commands
            .iter()
            .filter_map(|(sequence, _, command)| match command {
                AdapterCommand::CreateGroupConsumer {
                    consumer_id: actual,
                    ..
                } if actual == consumer_id => Some((*sequence, command)),
                _ => None,
            })
            .collect::<Vec<_>>();
        let exact = matches!(commands.as_slice(), [(_, actual)] if *actual == &expected);
        if exact {
            continue;
        }
        violations.push(violation(
            "CONS-024",
            format!(
                "group consumer {consumer_id} selected operation_config but observed {} matching creation command(s) without one exact aggregate policy",
                commands.len()
            ),
            None,
            commands
                .iter()
                .map(|(sequence, ..)| format!("history:{sequence}"))
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
        unreachable!("aggregate operation policy belongs to group creation");
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
