//! Configured assigned consumers require one exact immutable policy command.

use testlab_schema::{AdapterCommand, ClientId, Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for step in &scenario.steps {
        let ScenarioAction::CreateAssignedConsumerClient(expected) = &step.action else {
            continue;
        };
        let relevant = index
            .commands
            .iter()
            .filter(|(_, _, command)| creation_client_id(command) == Some(&expected.client_id))
            .collect::<Vec<_>>();
        let exact = relevant.len() == 1
            && relevant.iter().all(|(_, _, command)| {
                matches!(
                    command,
                    AdapterCommand::CreateAssignedConsumerClient(actual) if actual == expected
                )
            });
        if exact {
            continue;
        }
        violations.push(violation(
            "CONS-027",
            format!(
                "configured assigned-consumer client {} expected one exact immutable policy command",
                expected.client_id
            ),
            None,
            relevant
                .into_iter()
                .map(|(sequence, _, _)| format!("history:{sequence}"))
                .collect(),
        ));
    }
}

fn creation_client_id(command: &AdapterCommand) -> Option<&ClientId> {
    match command {
        AdapterCommand::CreateClient(command) => Some(&command.client_id),
        AdapterCommand::CreateConfiguredClient(action) => Some(&action.client_id),
        AdapterCommand::CreateAssignedConsumerClient(action) => Some(&action.client_id),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use testlab_schema::{
        AdapterCommand, AssignedConsumerReadIsolation, CreateClientCommand, Scenario,
        ScenarioAction,
    };

    use super::verify;
    use crate::index::HistoryIndex;
    use crate::verify_fixture::command;

    #[test]
    fn exact_assigned_consumer_policy_is_required_once() {
        let scenario = scenario();
        let ScenarioAction::CreateAssignedConsumerClient(expected) = &scenario.steps[0].action
        else {
            panic!("configured assigned-consumer fixture");
        };
        let exact = vec![command(
            0,
            AdapterCommand::CreateAssignedConsumerClient(expected.clone()),
        )];
        assert!(violations(&scenario, &exact).is_empty());

        let mut wrong_isolation = expected.clone();
        wrong_isolation.configuration.read_isolation =
            AssignedConsumerReadIsolation::ReadUncommitted;
        assert_contract(&violations(
            &scenario,
            &[command(
                0,
                AdapterCommand::CreateAssignedConsumerClient(wrong_isolation),
            )],
        ));

        let mut wrong_fetch = expected.clone();
        wrong_fetch
            .configuration
            .fetch
            .as_mut()
            .expect("fixture Fetch policy")
            .max_wait_ms += 1;
        assert_contract(&violations(
            &scenario,
            &[command(
                0,
                AdapterCommand::CreateAssignedConsumerClient(wrong_fetch),
            )],
        ));

        let mut wrong_limits = expected.clone();
        wrong_limits
            .configuration
            .limits
            .as_mut()
            .expect("fixture limits policy")
            .buffered_batches += 1;
        assert_contract(&violations(
            &scenario,
            &[command(
                0,
                AdapterCommand::CreateAssignedConsumerClient(wrong_limits),
            )],
        ));

        assert_contract(&violations(
            &scenario,
            &[command(
                0,
                AdapterCommand::CreateClient(CreateClientCommand {
                    client_id: expected.client_id.clone(),
                    expected_cluster_id: None,
                }),
            )],
        ));

        let producer_scenario: Scenario = toml::from_str(include_str!(
            "../../../scenarios/kafka/producer-configuration-zstd.toml"
        ))
        .unwrap_or_else(|error| panic!("parse configured producer: {error}"));
        let ScenarioAction::CreateConfiguredClient(mut producer) =
            producer_scenario.steps[0].action.clone()
        else {
            panic!("configured producer fixture");
        };
        producer.client_id = expected.client_id.clone();
        assert_contract(&violations(
            &scenario,
            &[command(0, AdapterCommand::CreateConfiguredClient(producer))],
        ));

        let mut duplicate = exact.clone();
        duplicate.push(command(
            1,
            AdapterCommand::CreateAssignedConsumerClient(expected.clone()),
        ));
        assert_contract(&violations(&scenario, &duplicate));
    }

    fn scenario() -> Scenario {
        toml::from_str(include_str!(
            "../../../scenarios/kafka/assigned-consumer-read-committed.toml"
        ))
        .unwrap_or_else(|error| panic!("parse configured assigned consumer: {error}"))
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
                .any(|value| value.contract_id.as_str() == "CONS-027"),
            "{violations:?}"
        );
    }
}
