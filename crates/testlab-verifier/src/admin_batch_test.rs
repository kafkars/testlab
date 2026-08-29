//! Batch topic tests pin ordered public outcomes and independent per-topic topology.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTopicCompletion, AdminTopicCreationOutcome,
    AdminTopicsCreationBatch, BrokerStateObservation, BrokerTopicState, Capability, ClientId,
    CreateTopicAction, CreateTopicBatchActionItem, CreateTopicBatchCommandItem, CreateTopicCommand,
    CreateTopicsBatchAction, CreateTopicsBatchCommand, HistoryEntry, HistoryPayload, OperationId,
    ScenarioAction, TOPIC_ALREADY_EXISTS_ERROR_CODE, TerminalStatus, VisibilityExpectation,
};

use super::verify;
use crate::verify_fixture::{adapter, command, event, history, scenario, step};

#[test]
fn mixed_batch_creation_passes_with_ordered_outcomes_and_exact_topology() {
    let (scenario, existing, batch) = batch_scenario();
    let events = batch_history(existing, batch, expected_outcomes(), vec![0, 1]);

    let verdict = verify(&scenario, &adapter(), &events, &[]);

    assert!(verdict.is_passed(), "{verdict:?}");
}

#[test]
fn reordered_per_topic_outcomes_fail_the_batch_contract() {
    let (scenario, existing, batch) = batch_scenario();
    let mut outcomes = expected_outcomes();
    outcomes.swap(0, 1);
    let events = batch_history(existing, batch, outcomes, vec![0, 1]);

    let verdict = verify(&scenario, &adapter(), &events, &[]);

    assert_contract(&verdict, "ADMIN-018");
}

#[test]
fn public_success_without_requested_broker_topology_fails_the_batch_contract() {
    let (scenario, existing, batch) = batch_scenario();
    let events = batch_history(existing, batch, expected_outcomes(), vec![0]);

    let verdict = verify(&scenario, &adapter(), &events, &[]);

    assert_contract(&verdict, "ADMIN-018");
}

fn batch_scenario() -> (testlab_schema::Scenario, OperationId, OperationId) {
    let mut value = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    value.requires.insert(Capability::Admin);
    let existing = operation("create-existing");
    let batch = operation("create-batch");
    value.steps.insert(
        2,
        step(
            "create-existing",
            ScenarioAction::CreateTopic(CreateTopicAction {
                client_id: client(),
                operation_id: existing.clone(),
                topic: "existing".to_owned(),
                partitions: 1,
                replication_factor: 1,
                validate_only: false,
                expected_error_code: None,
                timeout_ms: 500,
            }),
        ),
    );
    value.steps.insert(
        3,
        step(
            "create-batch",
            ScenarioAction::CreateTopicsBatch(CreateTopicsBatchAction {
                client_id: client(),
                operation_id: batch.clone(),
                topics: vec![
                    action_item("fresh", 2, None),
                    action_item(
                        "existing",
                        1,
                        Some(TOPIC_ALREADY_EXISTS_ERROR_CODE.to_owned()),
                    ),
                ],
                timeout_ms: 500,
            }),
        ),
    );
    (value, existing, batch)
}

fn batch_history(
    existing: OperationId,
    batch: OperationId,
    outcomes: Vec<AdminTopicCreationOutcome>,
    fresh_partitions: Vec<i32>,
) -> Vec<HistoryEntry> {
    let mut entries = history(TerminalStatus::Acknowledged);
    entries.push(command(
        10,
        AdapterCommand::CreateTopic(CreateTopicCommand {
            client_id: client(),
            operation_id: existing.clone(),
            topic: "existing".to_owned(),
            partitions: 1,
            replication_factor: 1,
            validate_only: false,
            timeout_ms: 500,
        }),
    ));
    entries.push(event(
        11,
        AdapterEvent::TopicCreated(AdminTopicCompletion {
            operation_id: existing.clone(),
            topic: "existing".to_owned(),
        }),
    ));
    entries.push(topic_state(12, existing, "existing", vec![0]));
    entries.push(command(
        13,
        AdapterCommand::CreateTopicsBatch(CreateTopicsBatchCommand {
            client_id: client(),
            operation_id: batch.clone(),
            topics: vec![command_item("fresh", 2), command_item("existing", 1)],
            timeout_ms: 500,
        }),
    ));
    entries.push(event(
        14,
        AdapterEvent::TopicsCreationCompleted(AdminTopicsCreationBatch {
            operation_id: batch.clone(),
            outcomes,
        }),
    ));
    entries.push(topic_state(15, batch.clone(), "fresh", fresh_partitions));
    entries.push(topic_state(16, batch, "existing", vec![0]));
    entries
}

fn expected_outcomes() -> Vec<AdminTopicCreationOutcome> {
    vec![
        AdminTopicCreationOutcome {
            topic: "fresh".to_owned(),
            error_code: None,
        },
        AdminTopicCreationOutcome {
            topic: "existing".to_owned(),
            error_code: Some(TOPIC_ALREADY_EXISTS_ERROR_CODE.to_owned()),
        },
    ]
}

fn action_item(
    topic: &str,
    partitions: i32,
    expected_error_code: Option<String>,
) -> CreateTopicBatchActionItem {
    CreateTopicBatchActionItem {
        topic: topic.to_owned(),
        partitions,
        replication_factor: 1,
        expected_error_code,
    }
}

fn command_item(topic: &str, partitions: i32) -> CreateTopicBatchCommandItem {
    CreateTopicBatchCommandItem {
        topic: topic.to_owned(),
        partitions,
        replication_factor: 1,
    }
}

fn topic_state(
    sequence: u64,
    operation_id: OperationId,
    topic: &str,
    partitions: Vec<i32>,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::Topic(BrokerTopicState {
                observation: sequence,
                operation_id,
                topic: topic.to_owned(),
                exists: true,
                partitions,
            }),
        },
    }
}

fn assert_contract(verdict: &testlab_schema::Verdict, contract: &str) {
    assert!(
        verdict
            .violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract),
        "{verdict:?}"
    );
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
