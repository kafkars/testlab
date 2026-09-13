//! Configured topic creation tests bind builder input to later independent value proof.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTopicConfigDescription, BrokerStateObservation,
    BrokerTopicConfigState, BrokerTopicState, Capability, CreateTopicAction, CreateTopicCommand,
    DescribeTopicConfigAction, DescribeTopicConfigCommand, HistoryEntry, HistoryPayload,
    OperationId, ScenarioAction, TerminalStatus, TopicCreationConfig, VisibilityExpectation,
};

use super::verify_topic_action;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

const TOPIC: &str = "configured-orders";
const CREATE: &str = "configured-create";
const DESCRIBE: &str = "configured-describe";

#[test]
fn configured_creation_requires_exact_later_public_and_independent_value() {
    let scenario = configured_scenario();
    let history = configured_history("compact");
    let mut violations = Vec::new();

    verify_topic_action(
        &scenario,
        &scenario.steps[2].action,
        &HistoryIndex::build(&history),
        &mut violations,
    );

    assert!(violations.is_empty(), "{violations:?}");

    let mut violations = Vec::new();
    verify_topic_action(
        &scenario,
        &scenario.steps[2].action,
        &HistoryIndex::build(&configured_history("delete")),
        &mut violations,
    );
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-094"),
        "{violations:?}"
    );
}

fn configured_scenario() -> testlab_schema::Scenario {
    let mut value = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    value.requires.insert(Capability::Admin);
    value
        .steps
        .insert(2, step("configured-create", create_action()));
    value.steps.insert(
        3,
        step(
            "configured-describe",
            ScenarioAction::DescribeTopicConfig(DescribeTopicConfigAction {
                client_id: client(),
                operation_id: operation(DESCRIBE),
                topic: TOPIC.to_owned(),
                config_name: "cleanup.policy".to_owned(),
                expected_value: "compact".to_owned(),
                timeout_ms: 1_000,
            }),
        ),
    );
    value
}

fn create_action() -> ScenarioAction {
    ScenarioAction::CreateTopic(CreateTopicAction {
        client_id: client(),
        operation_id: operation(CREATE),
        topic: TOPIC.to_owned(),
        partitions: 1,
        replication_factor: 1,
        replica_assignments: None,
        configs: vec![TopicCreationConfig {
            name: "cleanup.policy".to_owned(),
            value: "compact".to_owned(),
        }],
        validate_only: false,
        expected_error_code: None,
        timeout_ms: 1_000,
    })
}

fn configured_history(independent_value: &str) -> Vec<HistoryEntry> {
    vec![
        command(
            0,
            AdapterCommand::CreateTopic(CreateTopicCommand {
                client_id: client(),
                operation_id: operation(CREATE),
                topic: TOPIC.to_owned(),
                partitions: 1,
                replication_factor: 1,
                replica_assignments: None,
                configs: vec![TopicCreationConfig {
                    name: "cleanup.policy".to_owned(),
                    value: "compact".to_owned(),
                }],
                validate_only: false,
                timeout_ms: 1_000,
            }),
        ),
        event(
            1,
            AdapterEvent::TopicCreated(testlab_schema::AdminTopicCompletion {
                operation_id: operation(CREATE),
                topic: TOPIC.to_owned(),
            }),
        ),
        topic_state(2),
        command(
            3,
            AdapterCommand::DescribeTopicConfig(DescribeTopicConfigCommand {
                client_id: client(),
                operation_id: operation(DESCRIBE),
                topic: TOPIC.to_owned(),
                config_name: "cleanup.policy".to_owned(),
                timeout_ms: 1_000,
            }),
        ),
        event(
            4,
            AdapterEvent::TopicConfigDescribed(AdminTopicConfigDescription {
                operation_id: operation(DESCRIBE),
                topic: TOPIC.to_owned(),
                config_name: "cleanup.policy".to_owned(),
                value: Some("compact".to_owned()),
            }),
        ),
        config_state(5, independent_value),
    ]
}

fn topic_state(sequence: u64) -> HistoryEntry {
    broker_state(
        sequence,
        BrokerStateObservation::Topic(BrokerTopicState {
            observation: sequence,
            operation_id: operation(CREATE),
            topic: TOPIC.to_owned(),
            exists: true,
            partitions: vec![0],
        }),
    )
}

fn config_state(sequence: u64, value: &str) -> HistoryEntry {
    broker_state(
        sequence,
        BrokerStateObservation::TopicConfig(BrokerTopicConfigState {
            observation: sequence,
            operation_id: operation(DESCRIBE),
            topic: TOPIC.to_owned(),
            config_name: "cleanup.policy".to_owned(),
            value: value.to_owned(),
        }),
    )
}

fn broker_state(sequence: u64, observation: BrokerStateObservation) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation { observation },
    }
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}
