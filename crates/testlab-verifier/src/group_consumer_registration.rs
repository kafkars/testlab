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
            relevant.len() == 1 && relevant.iter().all(|(_, _, actual)| *actual == &expected);
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
mod tests {
    use testlab_schema::{AdapterCommand, ChildHandleOwnership, GroupProtocol, ScenarioAction};

    use super::{command, verify};
    use crate::index::HistoryIndex;
    use crate::verify_fixture::command as history_command;

    #[test]
    fn exact_complete_group_registration_is_required_once() {
        let scenario = scenario();
        let action = registration(&scenario);
        let ScenarioAction::CreateGroupConsumer {
            client_id,
            consumer_id,
            group_id,
            topics,
            ..
        } = action
        else {
            unreachable!("registration helper returns group creation");
        };
        let exact_command = command(action);
        let exact = vec![history_command(0, exact_command.clone())];
        assert!(violations(&scenario, &exact).is_empty());

        let mut wrong_topics = exact_command.clone();
        let AdapterCommand::CreateGroupConsumer { topics, .. } = &mut wrong_topics else {
            unreachable!("expected group command");
        };
        topics.push("substituted-topic".to_owned());
        assert_contract(&violations(&scenario, &[history_command(0, wrong_topics)]));

        let mut wrong_protocol = exact_command.clone();
        let AdapterCommand::CreateGroupConsumer { protocol, .. } = &mut wrong_protocol else {
            unreachable!("expected group command");
        };
        *protocol = GroupProtocol::Consumer;
        assert_contract(&violations(
            &scenario,
            &[history_command(0, wrong_protocol)],
        ));

        let mut wrong_policy = exact_command.clone();
        let AdapterCommand::CreateGroupConsumer { configuration, .. } = &mut wrong_policy else {
            unreachable!("expected group command");
        };
        configuration
            .as_mut()
            .expect("configured fixture")
            .processing_timeout_ms = Some(90_001);
        assert_contract(&violations(&scenario, &[history_command(0, wrong_policy)]));

        assert_contract(&violations(
            &scenario,
            &[history_command(
                0,
                AdapterCommand::CreateAssignedConsumer {
                    client_id: client_id.clone(),
                    consumer_id: consumer_id.clone(),
                    ownership: ChildHandleOwnership::Shared,
                },
            )],
        ));

        assert_contract(&violations(
            &scenario,
            &[history_command(
                0,
                AdapterCommand::CreateShareConsumer {
                    client_id: client_id.clone(),
                    consumer_id: consumer_id.clone(),
                    group_id: group_id.clone(),
                    topics: topics.clone(),
                    rack: None,
                    membership_timeout_ms: 30_000,
                    close_timeout_ms: 30_000,
                    configuration: None,
                },
            )],
        ));

        let mut duplicate = exact.clone();
        duplicate.push(history_command(1, exact_command));
        assert_contract(&violations(&scenario, &duplicate));
    }

    #[test]
    fn unconfigured_group_registration_is_also_exact() {
        let scenario: testlab_schema::Scenario = toml::from_str(include_str!(
            "../../../scenarios/kafka/admin-alter-consumer-group-offset-forward.toml"
        ))
        .unwrap_or_else(|error| panic!("parse unconfigured group: {error}"));
        let action = registration(&scenario);
        let history = [history_command(0, command(action))];
        assert!(violations(&scenario, &history).is_empty());
    }

    fn scenario() -> testlab_schema::Scenario {
        toml::from_str(include_str!(
            "../../../scenarios/kafka/classic-group-seek-replay.toml"
        ))
        .unwrap_or_else(|error| panic!("parse configured group: {error}"))
    }

    fn registration(scenario: &testlab_schema::Scenario) -> &ScenarioAction {
        scenario
            .steps
            .iter()
            .map(|step| &step.action)
            .find(|action| matches!(action, ScenarioAction::CreateGroupConsumer { .. }))
            .expect("group registration fixture")
    }

    fn violations(
        scenario: &testlab_schema::Scenario,
        history: &[testlab_schema::HistoryEntry],
    ) -> Vec<testlab_schema::Violation> {
        let mut violations = Vec::new();
        verify(scenario, &HistoryIndex::build(history), &mut violations);
        violations
    }

    fn assert_contract(violations: &[testlab_schema::Violation]) {
        assert!(
            violations
                .iter()
                .any(|value| value.contract_id.as_str() == "CONS-028"),
            "{violations:?}"
        );
    }
}
