//! Ordinary producer and assigned-consumer handles require exact creation commands.

use testlab_schema::{AdapterCommand, ConsumerId, ProducerId, Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    if index.commands.is_empty() {
        return;
    }
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::CreateProducer { producer_id, .. } => verify_one(
                producer_command(&step.action).expect("producer action"),
                index
                    .commands
                    .iter()
                    .filter(|(_, _, command)| producer_creation_id(command) == Some(producer_id)),
                "PROD-020",
                format!(
                    "producer {producer_id} expected one exact client, producer, and child-ownership creation command"
                ),
                same_producer_owner,
                violations,
            ),
            ScenarioAction::CreateAssignedConsumer { consumer_id, .. } => verify_one(
                assigned_command(&step.action).expect("assigned-consumer action"),
                index
                    .commands
                    .iter()
                    .filter(|(_, _, command)| consumer_creation_id(command) == Some(consumer_id)),
                "CONS-029",
                format!(
                    "assigned consumer {consumer_id} expected one exact client, consumer, and child-ownership creation command"
                ),
                same_command,
                violations,
            ),
            _ => {}
        }
    }
}

fn verify_one<'a>(
    expected: AdapterCommand,
    relevant: impl Iterator<Item = &'a (u64, testlab_schema::CommandId, AdapterCommand)>,
    contract: &str,
    message: String,
    matches: impl Fn(&AdapterCommand, &AdapterCommand) -> bool,
    violations: &mut Vec<Violation>,
) {
    let relevant = relevant.collect::<Vec<_>>();
    if relevant.len() == 1
        && relevant
            .iter()
            .all(|(_, _, actual)| matches(actual, &expected))
    {
        return;
    }
    violations.push(violation(
        contract,
        message,
        None,
        relevant
            .into_iter()
            .map(|(sequence, _, _)| format!("history:{sequence}"))
            .collect(),
    ));
}

fn same_producer_owner(actual: &AdapterCommand, expected: &AdapterCommand) -> bool {
    matches!(
        (actual, expected),
        (
            AdapterCommand::CreateProducer {
                client_id: actual_client,
                producer_id: actual_producer,
                ownership: actual_ownership,
                ..
            },
            AdapterCommand::CreateProducer {
                client_id: expected_client,
                producer_id: expected_producer,
                ownership: expected_ownership,
                ..
            }
        ) if actual_client == expected_client
            && actual_producer == expected_producer
            && actual_ownership == expected_ownership
    )
}

fn same_command(actual: &AdapterCommand, expected: &AdapterCommand) -> bool {
    actual == expected
}

fn producer_command(action: &ScenarioAction) -> Option<AdapterCommand> {
    let ScenarioAction::CreateProducer {
        client_id,
        producer_id,
        ownership,
        delivery_timeout_ms,
    } = action
    else {
        return None;
    };
    Some(AdapterCommand::CreateProducer {
        client_id: client_id.clone(),
        producer_id: producer_id.clone(),
        ownership: *ownership,
        delivery_timeout_ms: *delivery_timeout_ms,
    })
}

fn assigned_command(action: &ScenarioAction) -> Option<AdapterCommand> {
    let ScenarioAction::CreateAssignedConsumer {
        client_id,
        consumer_id,
        ownership,
    } = action
    else {
        return None;
    };
    Some(AdapterCommand::CreateAssignedConsumer {
        client_id: client_id.clone(),
        consumer_id: consumer_id.clone(),
        ownership: *ownership,
    })
}

fn producer_creation_id(command: &AdapterCommand) -> Option<&ProducerId> {
    match command {
        AdapterCommand::CreateProducer { producer_id, .. }
        | AdapterCommand::CreateTransactionalProducer { producer_id, .. }
        | AdapterCommand::FenceTransaction {
            replacement_producer_id: producer_id,
            ..
        } => Some(producer_id),
        _ => None,
    }
}

fn consumer_creation_id(command: &AdapterCommand) -> Option<&ConsumerId> {
    match command {
        AdapterCommand::CreateAssignedConsumer { consumer_id, .. }
        | AdapterCommand::CreateGroupConsumer { consumer_id, .. }
        | AdapterCommand::CreateShareConsumer { consumer_id, .. } => Some(consumer_id),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use testlab_schema::{
        AdapterCommand, ChildHandleOwnership, HistoryEntry, HistoryPayload, Scenario,
        ScenarioAction,
    };

    use super::{assigned_command, producer_command, verify};
    use crate::index::HistoryIndex;
    use crate::verify_fixture::command;

    #[test]
    fn exact_independent_producer_registrations_are_required_once() {
        let scenario = parse("producer-sibling-close-isolation.toml");
        let exact = creation_history(&scenario);
        assert!(violations(&scenario, &exact).is_empty());

        let mut wrong_owner = exact.clone();
        let AdapterCommand::CreateProducer { ownership, .. } = command_mut(&mut wrong_owner[0])
        else {
            panic!("producer command kind");
        };
        *ownership = ChildHandleOwnership::Shared;
        assert_contract(&violations(&scenario, &wrong_owner), "PROD-020");

        let mut substituted = exact.clone();
        let AdapterCommand::CreateProducer {
            client_id,
            producer_id,
            ..
        } = command_mut(&mut substituted[0]).clone()
        else {
            panic!("producer command kind");
        };
        *command_mut(&mut substituted[0]) = AdapterCommand::CreateTransactionalProducer {
            client_id,
            producer_id,
            transactional_id: "substituted".to_owned(),
            transaction_timeout_ms: 30_000,
            initialization_timeout_ms: 30_000,
        };
        assert_contract(&violations(&scenario, &substituted), "PROD-020");

        let mut duplicate = exact.clone();
        duplicate.push(exact[0].clone());
        assert_contract(&violations(&scenario, &duplicate), "PROD-020");
    }

    #[test]
    fn exact_independent_assigned_registrations_are_required_once() {
        let scenario = parse("assigned-consumer-independent-cursors.toml");
        let exact = creation_history(&scenario);
        assert!(violations(&scenario, &exact).is_empty());

        let mut wrong_owner = exact.clone();
        let AdapterCommand::CreateAssignedConsumer { ownership, .. } =
            command_mut(&mut wrong_owner[0])
        else {
            panic!("assigned-consumer command kind");
        };
        *ownership = ChildHandleOwnership::Shared;
        assert_contract(&violations(&scenario, &wrong_owner), "CONS-029");

        let mut substituted = exact.clone();
        let AdapterCommand::CreateAssignedConsumer {
            client_id,
            consumer_id,
            ..
        } = command_mut(&mut substituted[0]).clone()
        else {
            panic!("assigned-consumer command kind");
        };
        *command_mut(&mut substituted[0]) = AdapterCommand::CreateGroupConsumer {
            client_id,
            consumer_id,
            group_id: "substituted".to_owned(),
            topics: vec!["records".to_owned()],
            protocol: testlab_schema::GroupProtocol::Classic,
            configuration: None,
        };
        assert_contract(&violations(&scenario, &substituted), "CONS-029");

        let mut duplicate = exact.clone();
        duplicate.push(exact[0].clone());
        assert_contract(&violations(&scenario, &duplicate), "CONS-029");
    }

    #[test]
    fn shared_registrations_are_also_exact() {
        for scenario in [
            parse("producer-round-trip.toml"),
            parse("assigned-consumer-round-trip.toml"),
        ] {
            let history = creation_history(&scenario);
            assert!(violations(&scenario, &history).is_empty());
        }
    }

    fn creation_history(scenario: &Scenario) -> Vec<HistoryEntry> {
        scenario
            .steps
            .iter()
            .filter_map(|step| {
                producer_command(&step.action).or_else(|| assigned_command(&step.action))
            })
            .enumerate()
            .map(|(index, command_value)| command(index as u64, command_value))
            .collect()
    }

    fn parse(name: &str) -> Scenario {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../scenarios/kafka")
            .join(name);
        toml::from_str(
            &std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display())),
        )
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
    }

    fn command_mut(entry: &mut HistoryEntry) -> &mut AdapterCommand {
        let HistoryPayload::Command(envelope) = &mut entry.payload else {
            panic!("command history entry");
        };
        &mut envelope.command
    }

    fn violations(scenario: &Scenario, history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
        let mut violations = Vec::new();
        verify(scenario, &HistoryIndex::build(history), &mut violations);
        violations
    }

    fn assert_contract(violations: &[testlab_schema::Violation], contract: &str) {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contract_id.as_str() == contract),
            "{violations:?}"
        );
    }
}
