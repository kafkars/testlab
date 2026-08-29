//! Admin verifier tests distinguish exact topic success from mismatched claims.

use testlab_schema::{
    AdapterCommand, AdapterEvent, BrokerStateObservation, BrokerTopicState, Capability,
    CreatePartitionsAction, CreatePartitionsCommand, CreateTopicAction, CreateTopicCommand,
    HistoryEntry, HistoryPayload, OperationId, ScenarioAction, TerminalStatus,
    VisibilityExpectation,
};

use super::verify;
use crate::verify_fixture::{adapter, command, event, history, scenario, step};

#[test]
fn exact_admin_topic_completion_passes() {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.requires.insert(Capability::Admin);
    let operation_id = id(OperationId::new("admin-create-1"));
    scenario.steps.insert(
        2,
        step(
            "admin-create",
            ScenarioAction::CreateTopic(CreateTopicAction {
                client_id: id(testlab_schema::ClientId::new("client-1")),
                operation_id: operation_id.clone(),
                topic: "records".to_owned(),
                partitions: 1,
                replication_factor: 1,
                validate_only: false,
                expected_error_code: None,
                timeout_ms: 1_000,
            }),
        ),
    );
    let mut events = history(TerminalStatus::Acknowledged);
    events.push(command(
        10,
        AdapterCommand::CreateTopic(CreateTopicCommand {
            client_id: id(testlab_schema::ClientId::new("client-1")),
            operation_id: operation_id.clone(),
            topic: "records".to_owned(),
            partitions: 1,
            replication_factor: 1,
            validate_only: false,
            timeout_ms: 1_000,
        }),
    ));
    events.push(event(
        11,
        AdapterEvent::TopicCreated(testlab_schema::AdminTopicCompletion {
            operation_id: operation_id.clone(),
            topic: "records".to_owned(),
        }),
    ));
    events.push(topic_state(12, operation_id, vec![0]));

    let verdict = verify(&scenario, &adapter(), &events, &[]);

    assert!(verdict.is_passed(), "{verdict:?}");
}

#[test]
fn exact_admin_partition_completion_passes() {
    let (scenario, operation_id) = partition_scenario();
    let mut events = history(TerminalStatus::Acknowledged);
    events.push(partition_command(10, operation_id.clone()));
    events.push(event(
        11,
        AdapterEvent::TopicPartitionsCreated(testlab_schema::AdminTopicCompletion {
            operation_id: operation_id.clone(),
            topic: "records".to_owned(),
        }),
    ));
    events.push(topic_state(12, operation_id, vec![0, 1]));

    let verdict = verify(&scenario, &adapter(), &events, &[]);

    assert!(verdict.is_passed(), "{verdict:?}");
}

#[test]
fn missing_admin_partition_completion_fails() {
    let (scenario, operation_id) = partition_scenario();
    let mut events = history(TerminalStatus::Acknowledged);
    events.push(partition_command(10, operation_id.clone()));
    events.push(topic_state(11, operation_id, vec![0, 1]));

    let verdict = verify(&scenario, &adapter(), &events, &[]);

    assert!(
        verdict
            .violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-002"),
        "{verdict:?}"
    );
}

#[test]
fn duplicate_admin_partition_completion_fails() {
    let (scenario, operation_id) = partition_scenario();
    let completion = AdapterEvent::TopicPartitionsCreated(testlab_schema::AdminTopicCompletion {
        operation_id: operation_id.clone(),
        topic: "records".to_owned(),
    });
    let mut events = history(TerminalStatus::Acknowledged);
    events.push(partition_command(10, operation_id.clone()));
    events.push(event(11, completion.clone()));
    events.push(event(12, completion));
    events.push(topic_state(13, operation_id, vec![0, 1]));

    let verdict = verify(&scenario, &adapter(), &events, &[]);

    assert!(
        verdict
            .violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-002"),
        "{verdict:?}"
    );
}

fn partition_scenario() -> (testlab_schema::Scenario, OperationId) {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.requires.insert(Capability::Admin);
    let operation_id = id(OperationId::new("admin-partitions-1"));
    scenario.steps.insert(
        2,
        step(
            "admin-partitions",
            ScenarioAction::CreatePartitions(CreatePartitionsAction {
                client_id: id(testlab_schema::ClientId::new("client-1")),
                operation_id: operation_id.clone(),
                topic: "records".to_owned(),
                total_count: 2,
                validate_only: false,
                expected_current_count: None,
                expected_error_code: None,
                timeout_ms: 1_000,
            }),
        ),
    );
    (scenario, operation_id)
}

fn partition_command(sequence: u64, operation_id: OperationId) -> HistoryEntry {
    command(
        sequence,
        AdapterCommand::CreatePartitions(CreatePartitionsCommand {
            client_id: id(testlab_schema::ClientId::new("client-1")),
            operation_id,
            topic: "records".to_owned(),
            total_count: 2,
            validate_only: false,
            timeout_ms: 1_000,
        }),
    )
}

fn topic_state(sequence: u64, operation_id: OperationId, partitions: Vec<i32>) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::Topic(BrokerTopicState {
                observation: sequence,
                operation_id,
                topic: "records".to_owned(),
                exists: true,
                partitions,
            }),
        },
    }
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
