//! Client metrics preserve every exact expectation-free observation request.

use testlab_schema::{
    AdapterCommand, ObserveClientMetricsCommand, Scenario, ScenarioAction, Violation,
};

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
        .filter(|(_, _, command)| matches!(command, AdapterCommand::ObserveClientMetrics(_)))
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
        "METRICS-004",
        format!(
            "client metrics expected {} exact ordered client and operation command(s)",
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
    let ScenarioAction::ObserveClientMetrics(action) = action else {
        return None;
    };
    Some(AdapterCommand::ObserveClientMetrics(
        ObserveClientMetricsCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
        },
    ))
}

#[cfg(test)]
mod tests {
    use testlab_schema::{
        AdapterCommand, ClientId, HistoryEntry, HistoryPayload, ObserveClientMetricsCommand,
        OperationId, Scenario, ScenarioAction, StepId,
    };

    use super::{command, verify};
    use crate::index::HistoryIndex;
    use crate::verify_fixture::command as history_command;

    #[test]
    fn repeated_metrics_requests_preserve_exact_order_and_identities() {
        let scenario = repeated_scenario();
        let exact = metrics_history(&scenario);
        assert_eq!(exact.len(), 2);
        assert!(violations(&scenario, &exact).is_empty());

        let mut wrong_identity = exact.clone();
        let command = command_mut(&mut wrong_identity[0]);
        command.client_id = ClientId::new("substituted-client").expect("client ID");
        command.operation_id = OperationId::new("substituted-operation").expect("operation ID");
        assert_contract(&violations(&scenario, &wrong_identity));

        let mut reordered = exact.clone();
        reordered.swap(0, 1);
        assert_contract(&violations(&scenario, &reordered));

        let mut missing = exact.clone();
        missing.pop();
        assert_contract(&violations(&scenario, &missing));

        let duplicate_command = exact[0].clone();
        let mut duplicate = exact;
        duplicate.push(duplicate_command);
        assert_contract(&violations(&scenario, &duplicate));
    }

    fn repeated_scenario() -> Scenario {
        let mut scenario = parse("client-metrics-producer.toml");
        let index = scenario
            .steps
            .iter()
            .position(|step| matches!(&step.action, ScenarioAction::ObserveClientMetrics(_)))
            .expect("metrics action");
        let mut repeated = scenario.steps[index].clone();
        repeated.id = StepId::new("observe-metrics-again").expect("step ID");
        let ScenarioAction::ObserveClientMetrics(action) = &mut repeated.action else {
            panic!("metrics action");
        };
        action.operation_id = OperationId::new("metrics-snapshot-again").expect("operation ID");
        scenario.steps.insert(index + 1, repeated);
        scenario
    }

    fn metrics_history(scenario: &Scenario) -> Vec<HistoryEntry> {
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

    fn command_mut(entry: &mut HistoryEntry) -> &mut ObserveClientMetricsCommand {
        let HistoryPayload::Command(envelope) = &mut entry.payload else {
            panic!("command history entry");
        };
        let AdapterCommand::ObserveClientMetrics(command) = &mut envelope.command else {
            panic!("client metrics command");
        };
        command
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
                .any(|violation| violation.contract_id.as_str() == "METRICS-004"),
            "{violations:?}"
        );
    }
}
