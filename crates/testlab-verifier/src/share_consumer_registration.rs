//! Every Share member requires one exact public registration command.

use testlab_schema::{AdapterCommand, ConsumerId, Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    if index.commands.is_empty() {
        return;
    }
    for step in &scenario.steps {
        let ScenarioAction::CreateShareConsumer { consumer_id, .. } = &step.action else {
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
            "SHARE-012",
            format!(
                "Share consumer {consumer_id} expected one exact client, group, ordered subscription, rack, deadline, and acquisition-policy registration command"
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
    let ScenarioAction::CreateShareConsumer {
        client_id,
        consumer_id,
        group_id,
        topics,
        rack,
        membership_timeout_ms,
        close_timeout_ms,
        configuration,
    } = action
    else {
        unreachable!("Share registration command requires Share creation");
    };
    AdapterCommand::CreateShareConsumer {
        client_id: client_id.clone(),
        consumer_id: consumer_id.clone(),
        group_id: group_id.clone(),
        topics: topics.clone(),
        rack: rack.clone(),
        membership_timeout_ms: *membership_timeout_ms,
        close_timeout_ms: *close_timeout_ms,
        configuration: *configuration,
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
    use testlab_schema::{AdapterCommand, GroupProtocol, Scenario, ScenarioAction};

    use super::{command, verify};
    use crate::index::HistoryIndex;
    use crate::verify_fixture::command as history_command;

    #[test]
    fn exact_complete_share_registration_is_required_once() {
        let scenario = configured_scenario();
        let action = registration(&scenario);
        let ScenarioAction::CreateShareConsumer {
            client_id,
            consumer_id,
            group_id,
            topics,
            ..
        } = action
        else {
            unreachable!("registration helper returns Share creation");
        };
        let exact_command = command(action);
        let exact = vec![history_command(0, exact_command.clone())];
        assert!(violations(&scenario, &exact).is_empty());

        let mut wrong_topics = exact_command.clone();
        let AdapterCommand::CreateShareConsumer {
            topics: wrong_topic_names,
            ..
        } = &mut wrong_topics
        else {
            unreachable!("expected Share command");
        };
        wrong_topic_names.push("substituted-topic".to_owned());
        assert_contract(&violations(&scenario, &[history_command(0, wrong_topics)]));

        let mut wrong_rack = exact_command.clone();
        let AdapterCommand::CreateShareConsumer { rack, .. } = &mut wrong_rack else {
            unreachable!("expected Share command");
        };
        *rack = Some("substituted-rack".to_owned());
        assert_contract(&violations(&scenario, &[history_command(0, wrong_rack)]));

        let mut wrong_deadline = exact_command.clone();
        let AdapterCommand::CreateShareConsumer {
            membership_timeout_ms,
            ..
        } = &mut wrong_deadline
        else {
            unreachable!("expected Share command");
        };
        *membership_timeout_ms += 1;
        assert_contract(&violations(
            &scenario,
            &[history_command(0, wrong_deadline)],
        ));

        let mut wrong_policy = exact_command.clone();
        let AdapterCommand::CreateShareConsumer { configuration, .. } = &mut wrong_policy else {
            unreachable!("expected Share command");
        };
        configuration
            .as_mut()
            .unwrap_or_else(|| panic!("configured fixture"))
            .max_records += 1;
        assert_contract(&violations(&scenario, &[history_command(0, wrong_policy)]));

        assert_contract(&violations(
            &scenario,
            &[history_command(
                0,
                AdapterCommand::CreateGroupConsumer {
                    client_id: client_id.clone(),
                    consumer_id: consumer_id.clone(),
                    group_id: group_id.clone(),
                    topics: topics.clone(),
                    protocol: GroupProtocol::Consumer,
                    configuration: None,
                },
            )],
        ));

        let mut duplicate = exact.clone();
        duplicate.push(history_command(1, exact_command));
        assert_contract(&violations(&scenario, &duplicate));
    }

    #[test]
    fn rack_and_multi_topic_registrations_are_exact() {
        for scenario in [
            toml::from_str(include_str!(
                "../../../scenarios/kafka/admin-describe-share-group.toml"
            ))
            .unwrap_or_else(|error| panic!("parse rack Share scenario: {error}")),
            toml::from_str(include_str!(
                "../../../scenarios/kafka/share-group-multi-topic-subscription.toml"
            ))
            .unwrap_or_else(|error| panic!("parse multi-topic Share scenario: {error}")),
        ] {
            let history = [history_command(0, command(registration(&scenario)))];
            assert!(violations(&scenario, &history).is_empty());
        }
    }

    fn configured_scenario() -> Scenario {
        toml::from_str(include_str!(
            "../../../scenarios/kafka/share-group-fetch-max-records.toml"
        ))
        .unwrap_or_else(|error| panic!("parse configured Share scenario: {error}"))
    }

    fn registration(scenario: &Scenario) -> &ScenarioAction {
        scenario
            .steps
            .iter()
            .map(|step| &step.action)
            .find(|action| matches!(action, ScenarioAction::CreateShareConsumer { .. }))
            .unwrap_or_else(|| panic!("Share registration fixture"))
    }

    fn violations(
        scenario: &Scenario,
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
                .any(|value| value.contract_id.as_str() == "SHARE-012"),
            "{violations:?}"
        );
    }
}
