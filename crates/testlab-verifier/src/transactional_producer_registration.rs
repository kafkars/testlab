//! Transactional producer initialization requires exact caller-selected construction input.

use std::collections::BTreeMap;

use testlab_schema::{AdapterCommand, ProducerId, Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    if index.commands.is_empty() {
        return;
    }
    let mut expected = BTreeMap::<ProducerId, Vec<AdapterCommand>>::new();
    for step in &scenario.steps {
        if let Some((producer_id, command)) = command(&step.action) {
            expected.entry(producer_id).or_default().push(command);
        }
    }
    for (producer_id, commands) in expected {
        let actual = index
            .commands
            .iter()
            .filter(|(_, _, command)| creation_producer_id(command) == Some(&producer_id))
            .collect::<Vec<_>>();
        let exact = actual.len() == commands.len()
            && actual
                .iter()
                .zip(&commands)
                .all(|((_, _, actual), expected)| *actual == expected);
        if exact {
            continue;
        }
        violations.push(violation(
            "TXN-011",
            format!(
                "transactional producer {producer_id} expected {} exact ordered client, identity, transaction-timeout, and initialization-deadline command(s)",
                commands.len()
            ),
            None,
            actual
                .into_iter()
                .map(|(sequence, _, _)| format!("history:{sequence}"))
                .collect(),
        ));
    }
}

fn command(action: &ScenarioAction) -> Option<(ProducerId, AdapterCommand)> {
    let ScenarioAction::CreateTransactionalProducer {
        client_id,
        producer_id,
        transactional_id,
        transaction_timeout_ms,
        initialization_timeout_ms,
        ..
    } = action
    else {
        return None;
    };
    Some((
        producer_id.clone(),
        AdapterCommand::CreateTransactionalProducer {
            client_id: client_id.clone(),
            producer_id: producer_id.clone(),
            transactional_id: transactional_id.clone(),
            transaction_timeout_ms: *transaction_timeout_ms,
            initialization_timeout_ms: *initialization_timeout_ms,
        },
    ))
}

fn creation_producer_id(command: &AdapterCommand) -> Option<&ProducerId> {
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

#[cfg(test)]
mod tests {
    use testlab_schema::{
        AdapterCommand, ChildHandleOwnership, HistoryEntry, HistoryPayload, Scenario,
    };

    use super::{command, verify};
    use crate::index::HistoryIndex;
    use crate::verify_fixture::command as history_command;

    #[test]
    fn exact_transactional_identity_and_timeouts_are_required_once() {
        let scenario = parse("transaction-batch-commit.toml");
        let exact = creation_history(&scenario);
        assert!(violations(&scenario, &exact).is_empty());

        let mut wrong_identity = exact.clone();
        let AdapterCommand::CreateTransactionalProducer {
            transactional_id, ..
        } = command_mut(&mut wrong_identity[0])
        else {
            panic!("transactional producer command kind");
        };
        *transactional_id = "substituted".to_owned();
        assert_contract(&violations(&scenario, &wrong_identity));

        let mut wrong_deadlines = exact.clone();
        let AdapterCommand::CreateTransactionalProducer {
            transaction_timeout_ms,
            initialization_timeout_ms,
            ..
        } = command_mut(&mut wrong_deadlines[0])
        else {
            panic!("transactional producer command kind");
        };
        *transaction_timeout_ms += 1;
        *initialization_timeout_ms += 1;
        assert_contract(&violations(&scenario, &wrong_deadlines));

        let mut substituted = exact.clone();
        let AdapterCommand::CreateTransactionalProducer {
            client_id,
            producer_id,
            ..
        } = command_mut(&mut substituted[0]).clone()
        else {
            panic!("transactional producer command kind");
        };
        *command_mut(&mut substituted[0]) = AdapterCommand::CreateProducer {
            client_id,
            producer_id,
            ownership: ChildHandleOwnership::Shared,
        };
        assert_contract(&violations(&scenario, &substituted));

        let mut duplicate = exact.clone();
        duplicate.push(exact[0].clone());
        assert_contract(&violations(&scenario, &duplicate));
    }

    #[test]
    fn repeated_denied_and_recovered_initialization_is_preserved_in_order() {
        let scenario = parse("transactional-id-authorization-recovery.toml");
        let exact = creation_history(&scenario);
        assert_eq!(exact.len(), 2);
        assert!(violations(&scenario, &exact).is_empty());

        let mut missing_recovery = exact;
        missing_recovery.pop();
        assert_contract(&violations(&scenario, &missing_recovery));
    }

    fn creation_history(scenario: &Scenario) -> Vec<HistoryEntry> {
        scenario
            .steps
            .iter()
            .filter_map(|step| command(&step.action).map(|(_, command)| command))
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
                .any(|violation| violation.contract_id.as_str() == "TXN-011"),
            "{violations:?}"
        );
    }
}
