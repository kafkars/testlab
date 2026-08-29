//! Topic-configuration schema tests pin expectation ownership and mutation preconditions.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AdapterCommand, AdapterEvent, AdminTopicConfigDescription, AlterTopicConfigAction,
    BrokerStateObservation, BrokerTopicConfigState, Capability, ClientId,
    DescribeTopicConfigAction, DescribeTopicConfigCommand, EVIDENCE_SCHEMA_VERSION, OperationId,
    PROTOCOL_VERSION, SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction, ScenarioId, ScenarioStep,
    StepId,
};

#[test]
fn description_expectation_does_not_cross_the_wire_boundary() {
    let action = ScenarioAction::DescribeTopicConfig(description("delete"));
    let command = AdapterCommand::DescribeTopicConfig(DescribeTopicConfigCommand {
        client_id: client(),
        operation_id: operation("config-describe"),
        topic: "orders".to_owned(),
        config_name: "cleanup.policy".to_owned(),
        timeout_ms: 1_000,
    });
    let event = AdapterEvent::TopicConfigDescribed(AdminTopicConfigDescription {
        operation_id: operation("config-describe"),
        topic: "orders".to_owned(),
        config_name: "cleanup.policy".to_owned(),
        value: Some("delete".to_owned()),
    });

    let action = encode(&action);
    let command = encode(&command);
    let event = encode(&event);
    assert!(action.contains("expected_value = \"delete\""));
    assert!(!command.contains("expected_value"));
    assert!(event.contains("value = \"delete\""));
}

#[test]
fn config_protocol_and_independent_evidence_versions_are_explicit() {
    assert_eq!(PROTOCOL_VERSION, 34);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 37);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 26);
    let observation = BrokerStateObservation::TopicConfig(BrokerTopicConfigState {
        observation: 7,
        operation_id: operation("config-describe"),
        topic: "orders".to_owned(),
        config_name: "cleanup.policy".to_owned(),
        value: "delete".to_owned(),
    });
    let encoded = encode(&observation);
    assert!(encoded.contains("kind = \"topic_config\""));
    assert!(encoded.contains("value = \"delete\""));
}

#[test]
fn config_action_validation_rejects_empty_values_and_reuses_shared_identity_rules() {
    let clients = BTreeMap::from([(client(), false)]);
    let mut operations = BTreeSet::new();
    let mut problems = Vec::new();
    crate::admin_config_action_validation::validate(
        &ScenarioAction::DescribeTopicConfig(description("")),
        &clients,
        &mut operations,
        &mut problems,
    );
    crate::admin_config_action_validation::validate(
        &ScenarioAction::AlterTopicConfig(AlterTopicConfigAction {
            client_id: client(),
            operation_id: operation("config-describe"),
            topic: "orders".to_owned(),
            config_name: String::new(),
            value: "compact".to_owned(),
            validate_only: false,
            expected_current_value: None,
            timeout_ms: 99,
        }),
        &clients,
        &mut operations,
        &mut problems,
    );

    for expected in [
        "configuration value must contain",
        "duplicate operation id",
        "invalid config_name",
        "timeout_ms must be between",
    ] {
        assert!(
            problems.iter().any(|problem| problem.contains(expected)),
            "missing {expected:?} in {problems:?}"
        );
    }
}

#[test]
fn alteration_requires_a_prior_different_description() {
    let alteration = ScenarioAction::AlterTopicConfig(AlterTopicConfigAction {
        client_id: client(),
        operation_id: operation("config-alter"),
        topic: "orders".to_owned(),
        config_name: "cleanup.policy".to_owned(),
        value: "compact".to_owned(),
        validate_only: false,
        expected_current_value: None,
        timeout_ms: 1_000,
    });
    let mut problems = Vec::new();
    crate::admin_config_transition_validation::validate(
        &scenario(vec![alteration.clone()]),
        &mut problems,
    );
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("prior topic-configuration"))
    );

    problems.clear();
    crate::admin_config_transition_validation::validate(
        &scenario(vec![
            ScenarioAction::DescribeTopicConfig(description("delete")),
            alteration.clone(),
        ]),
        &mut problems,
    );
    assert!(problems.is_empty(), "{problems:?}");

    problems.clear();
    let ScenarioAction::AlterTopicConfig(mut repeated) = alteration.clone() else {
        panic!("alteration action kind");
    };
    repeated.operation_id = operation("config-alter-again");
    repeated.value = "compact,delete".to_owned();
    crate::admin_config_transition_validation::validate(
        &scenario(vec![
            ScenarioAction::DescribeTopicConfig(description("delete")),
            alteration,
            ScenarioAction::AlterTopicConfig(repeated),
        ]),
        &mut problems,
    );
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("prior topic-configuration"))
    );
}

fn description(expected_value: &str) -> DescribeTopicConfigAction {
    DescribeTopicConfigAction {
        client_id: client(),
        operation_id: operation("config-describe"),
        topic: "orders".to_owned(),
        config_name: "cleanup.policy".to_owned(),
        expected_value: expected_value.to_owned(),
        timeout_ms: 1_000,
    }
}

fn scenario(actions: Vec<ScenarioAction>) -> Scenario {
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("config-test").unwrap_or_else(|error| panic!("scenario ID: {error}")),
        title: "config test".to_owned(),
        description: "config test".to_owned(),
        timeout_ms: 5_000,
        requires: BTreeSet::from([Capability::Admin]),
        steps: actions
            .into_iter()
            .enumerate()
            .map(|(index, action)| ScenarioStep {
                id: StepId::new(format!("step-{index}"))
                    .unwrap_or_else(|error| panic!("step ID: {error}")),
                action,
            })
            .collect(),
        assertions: Vec::new(),
    }
}

fn encode<T: serde::Serialize>(value: &T) -> String {
    toml::to_string(value).unwrap_or_else(|error| panic!("serialize config value: {error}"))
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}
