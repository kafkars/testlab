//! Repeated public lifecycle requests preserve the exact scenario command stream.

use testlab_schema::{AdapterCommand, Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    if index.commands.is_empty() {
        return;
    }
    let expected = scenario
        .steps
        .iter()
        .filter_map(|step| command(&step.action))
        .collect::<Vec<_>>();
    if expected.is_empty() {
        return;
    }
    let actual = index
        .commands
        .iter()
        .filter(|(_, _, command)| is_lifecycle_request(command))
        .collect::<Vec<_>>();
    let exact = actual.len() == expected.len()
        && actual
            .iter()
            .zip(&expected)
            .all(|((_, _, actual), expected)| *actual == expected);
    if exact {
        return;
    }
    violations.push(violation(
        "LIFE-017",
        format!(
            "lifecycle expected {} exact ordered readiness, flush, close, abandonment, and shutdown command(s)",
            expected.len()
        ),
        None,
        actual
            .into_iter()
            .map(|(sequence, _, _)| format!("history:{sequence}"))
            .collect(),
    ));
}

fn command(action: &ScenarioAction) -> Option<AdapterCommand> {
    match action {
        ScenarioAction::AwaitClientReady { client_id } => Some(AdapterCommand::AwaitClientReady {
            client_id: client_id.clone(),
        }),
        ScenarioAction::Flush { producer_id } => Some(AdapterCommand::Flush {
            producer_id: producer_id.clone(),
        }),
        ScenarioAction::CloseProducer { producer_id } => Some(AdapterCommand::CloseProducer {
            producer_id: producer_id.clone(),
        }),
        ScenarioAction::ShutdownClient { client_id } => Some(AdapterCommand::ShutdownClient {
            client_id: client_id.clone(),
        }),
        ScenarioAction::CloseAssignedConsumer { consumer_id } => {
            Some(AdapterCommand::CloseAssignedConsumer {
                consumer_id: consumer_id.clone(),
            })
        }
        ScenarioAction::CloseGroupConsumer { consumer_id } => {
            Some(AdapterCommand::CloseGroupConsumer {
                consumer_id: consumer_id.clone(),
            })
        }
        ScenarioAction::AbandonGroupConsumer(action) => {
            Some(AdapterCommand::AbandonGroupConsumer(action.clone()))
        }
        ScenarioAction::CloseTransactionalProducer(action) => {
            Some(AdapterCommand::CloseTransactionalProducer {
                producer_id: action.producer_id.clone(),
            })
        }
        _ => None,
    }
}

fn is_lifecycle_request(command: &AdapterCommand) -> bool {
    matches!(
        command,
        AdapterCommand::AwaitClientReady { .. }
            | AdapterCommand::Flush { .. }
            | AdapterCommand::CloseProducer { .. }
            | AdapterCommand::ShutdownClient { .. }
            | AdapterCommand::CloseAssignedConsumer { .. }
            | AdapterCommand::CloseGroupConsumer { .. }
            | AdapterCommand::AbandonGroupConsumer(_)
            | AdapterCommand::CloseTransactionalProducer { .. }
    )
}

#[cfg(test)]
mod tests {
    use testlab_schema::{AdapterCommand, ClientId, HistoryEntry, HistoryPayload, Scenario};

    use super::{command, verify};
    use crate::index::HistoryIndex;
    use crate::verify_fixture::command as history_command;

    #[test]
    fn repeated_readiness_flush_and_terminals_preserve_exact_stream() {
        let scenario = parse("producer-repeated-readiness-flush.toml");
        let exact = lifecycle_history(&scenario);
        assert_eq!(exact.len(), 9);
        assert!(violations(&scenario, &exact).is_empty());

        let mut wrong_identity = exact.clone();
        let AdapterCommand::AwaitClientReady { client_id } = command_mut(&mut wrong_identity[0])
        else {
            panic!("client readiness command");
        };
        *client_id = ClientId::new("substituted-client").expect("client ID");
        assert_contract(&violations(&scenario, &wrong_identity));

        let mut reordered = exact.clone();
        reordered.swap(0, 2);
        assert_contract(&violations(&scenario, &reordered));

        let mut missing = exact.clone();
        missing.remove(1);
        assert_contract(&violations(&scenario, &missing));

        let duplicate_command = exact[0].clone();
        let mut duplicate = exact;
        duplicate.push(duplicate_command);
        assert_contract(&violations(&scenario, &duplicate));
    }

    #[test]
    fn consumer_and_transaction_terminals_are_included() {
        for name in [
            "assigned-consumer-round-trip.toml",
            "classic-group-round-trip.toml",
            "transaction-multi-record-commit.toml",
            "admin-remove-static-group-members.toml",
        ] {
            let scenario = parse(name);
            let history = lifecycle_history(&scenario);
            assert!(!history.is_empty(), "{name}");
            assert!(violations(&scenario, &history).is_empty(), "{name}");
        }
    }

    fn lifecycle_history(scenario: &Scenario) -> Vec<HistoryEntry> {
        scenario
            .steps
            .iter()
            .filter_map(|step| command(&step.action))
            .enumerate()
            .map(|(index, command)| history_command(index as u64, command))
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

    fn assert_contract(violations: &[testlab_schema::Violation]) {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contract_id.as_str() == "LIFE-017"),
            "{violations:?}"
        );
    }
}
