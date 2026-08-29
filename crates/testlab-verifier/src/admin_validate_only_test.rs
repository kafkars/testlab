//! Validate-only verdict tests require distinct completions and unchanged broker snapshots.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTopicCompletion, AdminTopicConfigCompletion,
    AlterTopicConfigAction, AlterTopicConfigCommand, BrokerStateObservation,
    BrokerTopicConfigState, BrokerTopicState, CreatePartitionsAction, CreatePartitionsCommand,
    CreateTopicAction, CreateTopicCommand, HistoryEntry, HistoryPayload, OperationId,
    ScenarioAction, TerminalStatus, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

const TOPIC: &str = "validate-only-topic";
const CONFIG: &str = "cleanup.policy";

#[test]
fn exact_validate_only_operations_pass() {
    for (action, history) in [
        (create_topic_action(), create_topic_history()),
        (create_partitions_action(), create_partitions_history()),
        (alter_config_action(), alter_config_history()),
    ] {
        assert!(violations(action, &history).is_empty());
    }
}

#[test]
fn topic_validation_rejects_a_mutation_completion() {
    let mut history = create_topic_history();
    history[1] = event(
        1,
        AdapterEvent::TopicCreated(AdminTopicCompletion {
            operation_id: operation("validate-create"),
            topic: TOPIC.to_owned(),
        }),
    );

    assert_contract(&violations(create_topic_action(), &history), "ADMIN-020");
}

#[test]
fn partition_validation_rejects_mutated_state() {
    let mut history = create_partitions_history();
    topic_state_mut(&mut history, "validate-partitions").partitions = vec![0, 1, 2];

    assert_contract(
        &violations(create_partitions_action(), &history),
        "ADMIN-021",
    );
}

#[test]
fn config_validation_requires_an_independent_baseline() {
    let mut history = alter_config_history();
    history.remove(0);
    resequence(&mut history);

    assert_contract(&violations(alter_config_action(), &history), "ADMIN-022");
}

#[test]
fn config_validation_rejects_a_mutation_completion() {
    let mut history = alter_config_history();
    history.insert(
        3,
        event(
            3,
            AdapterEvent::TopicConfigAltered(AdminTopicConfigCompletion {
                operation_id: operation("validate-config"),
                topic: TOPIC.to_owned(),
                config_name: CONFIG.to_owned(),
            }),
        ),
    );
    resequence(&mut history);

    assert_contract(&violations(alter_config_action(), &history), "ADMIN-022");
}

fn create_topic_history() -> Vec<HistoryEntry> {
    let operation_id = operation("validate-create");
    vec![
        command(
            0,
            AdapterCommand::CreateTopic(CreateTopicCommand {
                client_id: client(),
                operation_id: operation_id.clone(),
                topic: TOPIC.to_owned(),
                partitions: 1,
                replication_factor: 1,
                validate_only: true,
                timeout_ms: 1_000,
            }),
        ),
        event(
            1,
            AdapterEvent::TopicCreationValidated(AdminTopicCompletion {
                operation_id: operation_id.clone(),
                topic: TOPIC.to_owned(),
            }),
        ),
        topic_state(2, operation_id, false, Vec::new()),
    ]
}

fn create_partitions_history() -> Vec<HistoryEntry> {
    let operation_id = operation("validate-partitions");
    vec![
        topic_state(0, operation("partition-baseline"), true, vec![0]),
        command(
            1,
            AdapterCommand::CreatePartitions(CreatePartitionsCommand {
                client_id: client(),
                operation_id: operation_id.clone(),
                topic: TOPIC.to_owned(),
                total_count: 3,
                validate_only: true,
                timeout_ms: 1_000,
            }),
        ),
        event(
            2,
            AdapterEvent::TopicPartitionIncreaseValidated(AdminTopicCompletion {
                operation_id: operation_id.clone(),
                topic: TOPIC.to_owned(),
            }),
        ),
        topic_state(3, operation_id, true, vec![0]),
    ]
}

fn alter_config_history() -> Vec<HistoryEntry> {
    let operation_id = operation("validate-config");
    vec![
        config_state(0, operation("config-baseline"), "delete"),
        command(
            1,
            AdapterCommand::AlterTopicConfig(AlterTopicConfigCommand {
                client_id: client(),
                operation_id: operation_id.clone(),
                topic: TOPIC.to_owned(),
                config_name: CONFIG.to_owned(),
                value: "compact".to_owned(),
                validate_only: true,
                timeout_ms: 1_000,
            }),
        ),
        event(
            2,
            AdapterEvent::TopicConfigAlterationValidated(AdminTopicConfigCompletion {
                operation_id: operation_id.clone(),
                topic: TOPIC.to_owned(),
                config_name: CONFIG.to_owned(),
            }),
        ),
        config_state(3, operation_id, "delete"),
    ]
}

fn create_topic_action() -> ScenarioAction {
    ScenarioAction::CreateTopic(CreateTopicAction {
        client_id: client(),
        operation_id: operation("validate-create"),
        topic: TOPIC.to_owned(),
        partitions: 1,
        replication_factor: 1,
        validate_only: true,
        expected_error_code: None,
        timeout_ms: 1_000,
    })
}

fn create_partitions_action() -> ScenarioAction {
    ScenarioAction::CreatePartitions(CreatePartitionsAction {
        client_id: client(),
        operation_id: operation("validate-partitions"),
        topic: TOPIC.to_owned(),
        total_count: 3,
        validate_only: true,
        expected_current_count: Some(1),
        expected_error_code: None,
        timeout_ms: 1_000,
    })
}

fn alter_config_action() -> ScenarioAction {
    ScenarioAction::AlterTopicConfig(AlterTopicConfigAction {
        client_id: client(),
        operation_id: operation("validate-config"),
        topic: TOPIC.to_owned(),
        config_name: CONFIG.to_owned(),
        value: "compact".to_owned(),
        validate_only: true,
        expected_current_value: Some("delete".to_owned()),
        timeout_ms: 1_000,
    })
}

fn violations(action: ScenarioAction, history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let mut value = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    value.steps.insert(2, step("validate-only", action));
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    verify_admin(&value, &index, &[], &mut violations);
    violations
}

fn topic_state(
    sequence: u64,
    operation_id: OperationId,
    exists: bool,
    partitions: Vec<i32>,
) -> HistoryEntry {
    state(
        sequence,
        BrokerStateObservation::Topic(BrokerTopicState {
            observation: sequence,
            operation_id,
            topic: TOPIC.to_owned(),
            exists,
            partitions,
        }),
    )
}

fn config_state(sequence: u64, operation_id: OperationId, value: &str) -> HistoryEntry {
    state(
        sequence,
        BrokerStateObservation::TopicConfig(BrokerTopicConfigState {
            observation: sequence,
            operation_id,
            topic: TOPIC.to_owned(),
            config_name: CONFIG.to_owned(),
            value: value.to_owned(),
        }),
    )
}

fn state(sequence: u64, observation: BrokerStateObservation) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation { observation },
    }
}

fn topic_state_mut<'a>(history: &'a mut [HistoryEntry], id: &str) -> &'a mut BrokerTopicState {
    history
        .iter_mut()
        .find_map(|entry| match &mut entry.payload {
            HistoryPayload::BrokerStateObservation {
                observation: BrokerStateObservation::Topic(value),
            } if value.operation_id.as_str() == id => Some(value),
            _ => None,
        })
        .unwrap_or_else(|| panic!("missing topic state {id}"))
}

fn resequence(history: &mut [HistoryEntry]) {
    for (sequence, entry) in history.iter_mut().enumerate() {
        entry.sequence = sequence as u64;
        entry.observed_unix_ms = sequence as u64;
    }
}

fn assert_contract(violations: &[testlab_schema::Violation], contract: &str) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract),
        "{violations:?}"
    );
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}
