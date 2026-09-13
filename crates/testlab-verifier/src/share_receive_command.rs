//! Share receives preserve each exact public acquisition request.

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
        .filter(|(_, _, command)| matches!(command, AdapterCommand::ShareReceive { .. }))
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
        "SHARE-013",
        format!(
            "Share receive expected {} exact ordered consumer, batch identity, and timeout command(s)",
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
    let ScenarioAction::ShareReceive {
        consumer_id,
        receive_id,
        timeout_ms,
        ..
    } = action
    else {
        return None;
    };
    Some(AdapterCommand::ShareReceive {
        consumer_id: consumer_id.clone(),
        receive_id: receive_id.clone(),
        timeout_ms: *timeout_ms,
    })
}

#[cfg(test)]
mod tests {
    use testlab_schema::{
        AdapterCommand, ConsumerId, HistoryEntry, HistoryPayload, OperationId, Scenario,
    };

    use super::{command, verify};
    use crate::index::HistoryIndex;
    use crate::verify_fixture::command as history_command;

    #[test]
    fn repeated_share_receives_preserve_exact_order_and_fields() {
        let scenario = parse("share-group-record-fidelity.toml");
        let exact = receive_history(&scenario);
        assert_eq!(exact.len(), 4);
        assert!(violations(&scenario, &exact).is_empty());

        let mut wrong_identity = exact.clone();
        let AdapterCommand::ShareReceive {
            consumer_id,
            receive_id,
            ..
        } = command_mut(&mut wrong_identity[0])
        else {
            panic!("Share receive command");
        };
        *consumer_id = ConsumerId::new("substituted-consumer").expect("consumer ID");
        *receive_id = OperationId::new("substituted-receive").expect("receive ID");
        assert_contract(&violations(&scenario, &wrong_identity));

        let mut wrong_timeout = exact.clone();
        let AdapterCommand::ShareReceive { timeout_ms, .. } = command_mut(&mut wrong_timeout[0])
        else {
            panic!("Share receive command");
        };
        *timeout_ms += 1;
        assert_contract(&violations(&scenario, &wrong_timeout));

        let mut reordered = exact.clone();
        reordered.swap(0, 1);
        assert_contract(&violations(&scenario, &reordered));

        let duplicate_command = exact[0].clone();
        let mut duplicate = exact;
        duplicate.push(duplicate_command);
        assert_contract(&violations(&scenario, &duplicate));
    }

    fn receive_history(scenario: &Scenario) -> Vec<HistoryEntry> {
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
                .any(|violation| violation.contract_id.as_str() == "SHARE-013"),
            "{violations:?}"
        );
    }
}
