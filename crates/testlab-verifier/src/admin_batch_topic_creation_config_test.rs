//! Configured batch tests bind exact command entries to later independent value proof.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTopicConfigDescription, AdminTopicCreationOutcome,
    AdminTopicsCreationBatch, BrokerStateObservation, BrokerTopicConfigState, BrokerTopicState,
    Capability, CreateTopicBatchActionItem, CreateTopicBatchCommandItem, CreateTopicsBatchAction,
    CreateTopicsBatchCommand, DescribeTopicConfigAction, DescribeTopicConfigCommand, HistoryEntry,
    HistoryPayload, OperationId, ScenarioAction, TerminalStatus, TopicCreationConfig,
    VisibilityExpectation,
};

use crate::verify_fixture::{admin_verdict, command, event, history, scenario, step};

const TOPIC: &str = "configured-batch-topic";
const BATCH: &str = "configured-batch-create";
const DESCRIBE: &str = "configured-batch-describe";

#[test]
fn configured_batch_requires_exact_command_and_later_independent_value() {
    let scenario = configured_scenario();
    let events = configured_history("compact");
    let verdict = admin_verdict(&scenario, &events);
    assert!(verdict.is_passed(), "{verdict:?}");

    let wrong_value = admin_verdict(&scenario, &configured_history("delete"));
    assert_contract(&wrong_value, "ADMIN-096");

    let mut missing_config = events;
    let HistoryPayload::HarnessCommand { command } = &mut missing_config[10].payload else {
        panic!("batch command history entry");
    };
    let AdapterCommand::CreateTopicsBatch(command) = &mut command.command else {
        panic!("batch command kind");
    };
    command.topics[0].configs.clear();
    assert_contract(&admin_verdict(&scenario, &missing_config), "ADMIN-096");
}

fn configured_scenario() -> testlab_schema::Scenario {
    let mut value = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    value.requires.insert(Capability::Admin);
    value.steps.insert(
        2,
        step(
            "configured-batch-create",
            ScenarioAction::CreateTopicsBatch(CreateTopicsBatchAction {
                client_id: client(),
                operation_id: operation(BATCH),
                topics: vec![action_item(TOPIC, true), action_item("batch-peer", false)],
                timeout_ms: 1_000,
            }),
        ),
    );
    value.steps.insert(
        3,
        step(
            "configured-batch-describe",
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

fn configured_history(independent_value: &str) -> Vec<HistoryEntry> {
    let mut entries = history(TerminalStatus::Acknowledged);
    entries.push(command(
        10,
        AdapterCommand::CreateTopicsBatch(CreateTopicsBatchCommand {
            client_id: client(),
            operation_id: operation(BATCH),
            topics: vec![command_item(TOPIC, true), command_item("batch-peer", false)],
            timeout_ms: 1_000,
        }),
    ));
    entries.push(event(
        11,
        AdapterEvent::TopicsCreationCompleted(AdminTopicsCreationBatch {
            operation_id: operation(BATCH),
            outcomes: vec![outcome(TOPIC), outcome("batch-peer")],
        }),
    ));
    entries.push(topic_state(12, TOPIC));
    entries.push(topic_state(13, "batch-peer"));
    entries.push(command(
        14,
        AdapterCommand::DescribeTopicConfig(DescribeTopicConfigCommand {
            client_id: client(),
            operation_id: operation(DESCRIBE),
            topic: TOPIC.to_owned(),
            config_name: "cleanup.policy".to_owned(),
            timeout_ms: 1_000,
        }),
    ));
    entries.push(event(
        15,
        AdapterEvent::TopicConfigDescribed(AdminTopicConfigDescription {
            operation_id: operation(DESCRIBE),
            topic: TOPIC.to_owned(),
            config_name: "cleanup.policy".to_owned(),
            value: Some("compact".to_owned()),
        }),
    ));
    entries.push(config_state(16, independent_value));
    entries
}

fn action_item(topic: &str, configured: bool) -> CreateTopicBatchActionItem {
    CreateTopicBatchActionItem {
        topic: topic.to_owned(),
        partitions: 1,
        replication_factor: 1,
        configs: configs(configured),
        expected_error_code: None,
    }
}

fn command_item(topic: &str, configured: bool) -> CreateTopicBatchCommandItem {
    CreateTopicBatchCommandItem {
        topic: topic.to_owned(),
        partitions: 1,
        replication_factor: 1,
        configs: configs(configured),
    }
}

fn configs(configured: bool) -> Vec<TopicCreationConfig> {
    configured
        .then(|| TopicCreationConfig {
            name: "cleanup.policy".to_owned(),
            value: "compact".to_owned(),
        })
        .into_iter()
        .collect()
}

fn outcome(topic: &str) -> AdminTopicCreationOutcome {
    AdminTopicCreationOutcome {
        topic: topic.to_owned(),
        error_code: None,
    }
}

fn topic_state(sequence: u64, topic: &str) -> HistoryEntry {
    broker_state(
        sequence,
        BrokerStateObservation::Topic(BrokerTopicState {
            observation: sequence,
            operation_id: operation(BATCH),
            topic: topic.to_owned(),
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

fn assert_contract(verdict: &testlab_schema::Verdict, contract: &str) {
    assert!(
        verdict
            .violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract),
        "{verdict:?}"
    );
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}
