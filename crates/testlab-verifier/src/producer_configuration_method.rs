//! Configured producer clients require one exact policy and public builder selection command.

use testlab_schema::{AdapterCommand, ClientId, Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for step in &scenario.steps {
        let ScenarioAction::CreateConfiguredClient(expected) = &step.action else {
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
                    AdapterCommand::CreateConfiguredClient(actual) if actual == expected
                )
            });
        if exact {
            continue;
        }
        violations.push(violation(
            "PROD-019",
            format!(
                "configured client {} expected one exact producer policy and public configuration method command",
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
        AdapterCommand, AdapterEvent, AdapterSecurity, ClientConfigurationObservation,
        CreateClientCommand, ProducerConfiguration, ProducerConfigurationMethod, RunId, Scenario,
        ScenarioAction, ScenarioId,
    };

    use super::verify;
    use crate::index::HistoryIndex;
    use crate::verify_fixture::{command, event};

    #[test]
    fn exact_method_and_policy_are_required_once() {
        let scenario = scenario();
        let ScenarioAction::CreateConfiguredClient(expected) = &scenario.steps[0].action else {
            panic!("configured client fixture");
        };
        let exact = vec![command(
            0,
            AdapterCommand::CreateConfiguredClient(expected.clone()),
        )];
        assert!(violations(&scenario, &exact).is_empty());

        let mut wrong_method = expected.clone();
        wrong_method.configuration_method = ProducerConfigurationMethod::ProducerConfig;
        assert_contract(&violations(
            &scenario,
            &[command(
                0,
                AdapterCommand::CreateConfiguredClient(wrong_method),
            )],
        ));

        let mut wrong_policy = expected.clone();
        wrong_policy.configuration.max_retries += 1;
        assert_contract(&violations(
            &scenario,
            &[command(
                0,
                AdapterCommand::CreateConfiguredClient(wrong_policy),
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

        let mut duplicate = exact.clone();
        duplicate.push(command(
            1,
            AdapterCommand::CreateConfiguredClient(expected.clone()),
        ));
        assert_contract(&violations(&scenario, &duplicate));
    }

    #[test]
    fn selected_public_policy_is_exact_correlated_unique_and_later() {
        let scenario = scenario();
        let expected = configuration(&scenario);
        let exact = client_history(&scenario, Some(expected));
        assert!(policy_violations(&scenario, &exact).is_empty());

        assert_producer_contract(&policy_violations(
            &scenario,
            &client_history(&scenario, None),
        ));

        let mut changed = expected;
        changed.max_retries += 1;
        assert_producer_contract(&policy_violations(
            &scenario,
            &client_history(&scenario, Some(changed)),
        ));

        let mut duplicate = exact.clone();
        duplicate.push(exact[2].clone());
        duplicate[3].sequence = 3;
        assert_producer_contract(&policy_violations(&scenario, &duplicate));

        let mut early = exact;
        early[2].sequence = 0;
        assert_producer_contract(&policy_violations(&scenario, &early));
    }

    fn client_history(
        scenario: &Scenario,
        selected_producer_configuration: Option<ProducerConfiguration>,
    ) -> Vec<testlab_schema::HistoryEntry> {
        let ScenarioAction::CreateConfiguredClient(action) = &scenario.steps[0].action else {
            panic!("configured client fixture");
        };
        vec![
            command(
                0,
                AdapterCommand::Hello {
                    run_id: RunId::new("run-1").unwrap_or_else(|error| panic!("run ID: {error}")),
                    scenario_id: ScenarioId::new("configured-client")
                        .unwrap_or_else(|error| panic!("scenario ID: {error}")),
                    broker_endpoints: vec!["127.0.0.1:9092".to_owned()],
                    security: AdapterSecurity::Plaintext,
                },
            ),
            command(1, AdapterCommand::CreateConfiguredClient(action.clone())),
            event(
                2,
                AdapterEvent::ClientCreated(ClientConfigurationObservation {
                    client_id: action.client_id.clone(),
                    observed_client_id: Some(action.client_id.as_str().to_owned()),
                    observed_bootstrap_servers: vec!["127.0.0.1:9092".to_owned()],
                    observed_expected_cluster_id: None,
                    selected_producer_configuration,
                }),
            ),
        ]
    }

    fn configuration(scenario: &Scenario) -> ProducerConfiguration {
        let ScenarioAction::CreateConfiguredClient(action) = &scenario.steps[0].action else {
            panic!("configured client fixture");
        };
        action.configuration
    }

    fn policy_violations(
        scenario: &Scenario,
        history: &[testlab_schema::HistoryEntry],
    ) -> Vec<testlab_schema::Violation> {
        let mut violations = Vec::new();
        crate::client_configuration::verify(
            scenario,
            &HistoryIndex::build(history),
            &mut violations,
        );
        violations
    }

    fn scenario() -> Scenario {
        toml::from_str(include_str!(
            "../../../scenarios/kafka/producer-configuration-zstd.toml"
        ))
        .unwrap_or_else(|error| panic!("parse configured producer: {error}"))
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
                .any(|value| value.contract_id.as_str() == "PROD-019"),
            "{violations:?}"
        );
    }

    fn assert_producer_contract(violations: &[testlab_schema::Violation]) {
        assert!(
            violations
                .iter()
                .any(|value| value.contract_id.as_str() == "PROD-024"),
            "{violations:?}"
        );
    }
}
