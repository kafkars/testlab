//! Producer receipt evidence must preserve identity, metadata, sizes, and chronology.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTopicDescriptionOutcome, AdminTopicDescriptionValue,
    AdminTopicPartitionDescriptionOutcome, AdminTopicsDescription, BrokerObservation,
    BrokerStateObservation, BrokerTopicIdentityState, HistoryEntry, HistoryPayload,
    ProducerReceipt, Scenario, ScenarioAction, TerminalStatus,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn exact_uuid_bound_receipts_pass() {
    let (history, observations) = evidence();
    assert!(violations(&history, &observations).is_empty());
}

#[test]
fn disabled_validation_or_wrong_receipt_id_fails() {
    let (mut disabled, observations) = evidence();
    *validation(&mut disabled, 0) = false;
    assert_contract(&violations(&disabled, &observations));

    let (mut wrong_id, observations) = evidence();
    receipt(&mut wrong_id, 0).topic_uuid = Some([9; 16]);
    assert_contract(&violations(&wrong_id, &observations));
}

#[test]
fn null_and_empty_serialized_sizes_are_exact() {
    let (mut null_key, observations) = evidence();
    receipt(&mut null_key, 0).serialized_key_size = Some(0);
    assert_contract(&violations(&null_key, &observations));

    let (mut null_value, observations) = evidence();
    receipt(&mut null_value, 1).serialized_value_size = Some(0);
    assert_contract(&violations(&null_value, &observations));
}

#[test]
fn late_identity_or_duplicate_terminal_fails() {
    let (mut late, observations) = evidence();
    late[2].sequence = 5;
    assert_contract(&violations(&late, &observations));

    let (mut duplicate, observations) = evidence();
    duplicate.push(duplicate[5].clone());
    assert_contract(&violations(&duplicate, &observations));
}

fn evidence() -> (Vec<HistoryEntry>, Vec<BrokerObservation>) {
    let scenario = scenario();
    let records = send_actions(&scenario)
        .into_iter()
        .map(|(_, record)| (*record).clone())
        .collect::<Vec<_>>();
    let history = vec![
        command(
            1,
            AdapterCommand::DescribeTopics(description_command(&scenario)),
        ),
        event(2, AdapterEvent::TopicsDescribed(description_value())),
        identity_state(3),
        command(4, send_command(&scenario, 0)),
        event(
            5,
            AdapterEvent::OperationAccepted {
                operation_id: operation(&scenario, 0),
            },
        ),
        terminal(&scenario, 0, 6, 0),
        command(7, send_command(&scenario, 1)),
        event(
            8,
            AdapterEvent::OperationAccepted {
                operation_id: operation(&scenario, 1),
            },
        ),
        terminal(&scenario, 1, 9, 1),
    ];
    let observations = records
        .into_iter()
        .enumerate()
        .map(|(index, record)| BrokerObservation {
            observation: u64::try_from(index)
                .unwrap_or_else(|error| panic!("observation: {error}")),
            offset: i64::try_from(index).unwrap_or_else(|error| panic!("offset: {error}")),
            operation_id: operation(&scenario, index),
            digest: record
                .digest()
                .unwrap_or_else(|error| panic!("record digest: {error}")),
            record,
        })
        .collect();
    (history, observations)
}

fn description_command(scenario: &Scenario) -> testlab_schema::DescribeTopicsCommand {
    let ScenarioAction::DescribeTopics(action) = &scenario.steps[3].action else {
        panic!("topic-ID description action")
    };
    testlab_schema::DescribeTopicsCommand {
        client_id: action.client_id.clone(),
        operation_id: action.operation_id.clone(),
        selection: action.selection,
        topics: action
            .topics
            .iter()
            .map(|value| value.topic.clone())
            .collect(),
        include_authorized_operations: action.include_authorized_operations,
        timeout_ms: action.timeout_ms,
    }
}

fn description_value() -> AdminTopicsDescription {
    AdminTopicsDescription {
        operation_id: identity_operation(),
        outcomes: vec![AdminTopicDescriptionOutcome {
            topic: topic().to_owned(),
            topic_id: Some(topic_id()),
            description: Some(AdminTopicDescriptionValue {
                topic_id: Some(topic_id()),
                internal: false,
                authorized_operations: None,
                partitions: vec![AdminTopicPartitionDescriptionOutcome {
                    partition: 0,
                    error_code: None,
                }],
            }),
            error_code: None,
        }],
    }
}

fn identity_state(sequence: u64) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::TopicIdentity(BrokerTopicIdentityState {
                observation: 40,
                operation_id: identity_operation(),
                topic: topic().to_owned(),
                topic_id: topic_id(),
                partitions: vec![0],
            }),
        },
    }
}

fn send_command(scenario: &Scenario, index: usize) -> AdapterCommand {
    let (action, record) = send_actions(scenario)[index];
    let ScenarioAction::Send {
        producer_id,
        operation_id,
        method,
        partitioning,
        ..
    } = action
    else {
        unreachable!()
    };
    AdapterCommand::Send {
        producer_id: producer_id.clone(),
        operation_id: operation_id.clone(),
        method: *method,
        partitioning: *partitioning,
        validate_topic_uuid: true,
        record: (*record).clone(),
    }
}

fn terminal(scenario: &Scenario, index: usize, sequence: u64, offset: i64) -> HistoryEntry {
    let (_, record) = send_actions(scenario)[index];
    event(
        sequence,
        AdapterEvent::OperationTerminal {
            operation_id: operation(scenario, index),
            status: TerminalStatus::Acknowledged,
            code: None,
            partition: Some(record.partition),
            offset: Some(offset),
            timestamp_millis: record.timestamp_millis,
            receipt: Some(ProducerReceipt {
                topic: record.topic.clone(),
                topic_uuid: Some(topic_id()),
                leader_epoch: Some(7),
                serialized_key_size: record.key.as_ref().map(|_| 0),
                serialized_value_size: record.value.as_ref().map(|_| 0),
            }),
        },
    )
}

fn send_actions(scenario: &Scenario) -> Vec<(&ScenarioAction, &testlab_schema::RecordSpec)> {
    scenario
        .steps
        .iter()
        .filter_map(|step| match &step.action {
            action @ ScenarioAction::Send { record, .. } => Some((action, record)),
            _ => None,
        })
        .collect()
}

fn operation(scenario: &Scenario, index: usize) -> testlab_schema::OperationId {
    let ScenarioAction::Send { operation_id, .. } = send_actions(scenario)[index].0 else {
        unreachable!()
    };
    operation_id.clone()
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/producer-receipt-metadata.toml"
    ))
    .unwrap_or_else(|error| panic!("parse producer receipt scenario: {error}"))
}

fn violations(
    entries: &[HistoryEntry],
    observations: &[BrokerObservation],
) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(entries);
    let mut violations = Vec::new();
    super::verify(&scenario(), &index, observations, &mut violations);
    violations
}

fn validation(entries: &mut [HistoryEntry], index: usize) -> &mut bool {
    let HistoryPayload::HarnessCommand { command } = &mut entries[3 + index * 3].payload else {
        panic!("send command")
    };
    let AdapterCommand::Send {
        validate_topic_uuid,
        ..
    } = &mut command.command
    else {
        panic!("send command")
    };
    validate_topic_uuid
}

fn receipt(entries: &mut [HistoryEntry], index: usize) -> &mut ProducerReceipt {
    let HistoryPayload::AdapterEvent { event } = &mut entries[5 + index * 3].payload else {
        panic!("terminal event")
    };
    let AdapterEvent::OperationTerminal {
        receipt: Some(receipt),
        ..
    } = &mut event.event
    else {
        panic!("producer receipt")
    };
    receipt
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "PROD-018"),
        "{violations:?}"
    );
}

fn identity_operation() -> testlab_schema::OperationId {
    testlab_schema::OperationId::new("producer-receipt-topic-identity")
        .unwrap_or_else(|error| panic!("identity operation: {error}"))
}

const fn topic_id() -> [u8; 16] {
    [7; 16]
}

const fn topic() -> &'static str {
    "testlab-producer-receipt-metadata"
}
