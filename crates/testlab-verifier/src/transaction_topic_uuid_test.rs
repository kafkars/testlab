//! UUID-bound transaction evidence must preserve identity, order, and chronology.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTopicDescriptionOutcome, AdminTopicDescriptionValue,
    AdminTopicPartitionDescriptionOutcome, AdminTopicsDescription, BrokerStateObservation,
    BrokerTopicIdentityState, DescribeTopicsCommand, HistoryEntry, HistoryPayload, Scenario,
    ScenarioAction, TransactionDisposition,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn exact_uuid_bound_transaction_evidence_passes() {
    assert!(violations(&history()).is_empty());
}

#[test]
fn disabled_validation_or_reordered_seal_fails() {
    let mut disabled = history();
    *transaction_validation(&mut disabled) = false;
    assert_contract(&violations(&disabled));

    let mut reordered = history();
    transaction_ids(&mut reordered).swap(0, 1);
    assert_contract(&violations(&reordered));
}

#[test]
fn substituted_or_late_independent_identity_fails() {
    let mut substituted = history();
    identity(&mut substituted, 0).topic_id = [9; 16];
    assert_contract(&violations(&substituted));

    let mut late = history();
    late[3].sequence = 6;
    assert_contract(&violations(&late));
}

#[test]
fn duplicate_completion_fails() {
    let mut duplicated = history();
    let completion = duplicated[9].clone();
    duplicated.push(completion);
    assert_contract(&violations(&duplicated));
}

fn history() -> Vec<HistoryEntry> {
    vec![
        command(1, AdapterCommand::DescribeTopics(description_command())),
        event(2, AdapterEvent::TopicsDescribed(description_value())),
        identity_state(3, 40, 0),
        identity_state(4, 41, 1),
        command(5, transaction_command_value()),
        operation_event(6, 0, true),
        operation_event(7, 0, false),
        operation_event(8, 1, true),
        operation_event(9, 1, false),
        event(
            10,
            AdapterEvent::TransactionCompleted {
                transaction_id: transaction_id(),
                disposition: TransactionDisposition::Commit,
                validated_topic_ids: vec![[1; 16], [2; 16]],
            },
        ),
    ]
}

fn description_command() -> DescribeTopicsCommand {
    let scenario = scenario();
    let ScenarioAction::DescribeTopics(action) = &scenario.steps[3].action else {
        panic!("topic-ID description action")
    };
    DescribeTopicsCommand {
        client_id: action.client_id.clone(),
        operation_id: action.operation_id.clone(),
        selection: action.selection,
        topics: action
            .topics
            .iter()
            .map(|value| value.topic.clone())
            .collect(),
        timeout_ms: action.timeout_ms,
    }
}

fn description_value() -> AdminTopicsDescription {
    AdminTopicsDescription {
        operation_id: identity_operation_id(),
        outcomes: topics()
            .into_iter()
            .enumerate()
            .map(|(index, topic)| description(topic, id(index)))
            .collect(),
    }
}

fn description(topic: &str, topic_id: [u8; 16]) -> AdminTopicDescriptionOutcome {
    AdminTopicDescriptionOutcome {
        topic: topic.to_owned(),
        topic_id: Some(topic_id),
        description: Some(AdminTopicDescriptionValue {
            topic_id: Some(topic_id),
            internal: false,
            partitions: vec![AdminTopicPartitionDescriptionOutcome {
                partition: 0,
                error_code: None,
            }],
        }),
        error_code: None,
    }
}

fn identity_state(sequence: u64, observation: u64, index: usize) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::TopicIdentity(BrokerTopicIdentityState {
                observation,
                operation_id: identity_operation_id(),
                topic: topics()[index].to_owned(),
                topic_id: id(index),
                partitions: vec![0],
            }),
        },
    }
}

fn transaction_command_value() -> AdapterCommand {
    let scenario = scenario();
    let ScenarioAction::ExecuteTransaction {
        producer_id,
        transaction_id,
        operations,
        method,
        disposition,
        timeout_ms,
        ..
    } = &scenario.steps[4].action
    else {
        panic!("UUID-bound transaction action")
    };
    AdapterCommand::ExecuteTransaction {
        producer_id: producer_id.clone(),
        transaction_id: transaction_id.clone(),
        operations: operations.clone(),
        method: *method,
        disposition: *disposition,
        validate_topic_uuids: true,
        timeout_ms: *timeout_ms,
    }
}

fn operation_event(sequence: u64, index: usize, accepted: bool) -> HistoryEntry {
    let operation_id = operation_ids()[index].clone();
    if accepted {
        event(sequence, AdapterEvent::OperationAccepted { operation_id })
    } else {
        event(
            sequence,
            AdapterEvent::OperationTerminal {
                operation_id,
                status: testlab_schema::TerminalStatus::TransactionStaged,
                code: None,
                partition: Some(0),
                offset: Some(
                    i64::try_from(index).unwrap_or_else(|error| panic!("record offset: {error}")),
                ),
                timestamp_millis: None,
            },
        )
    }
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/transaction-topic-uuid-commit.toml"
    ))
    .unwrap_or_else(|error| panic!("parse UUID-bound transaction scenario: {error}"))
}

fn violations(entries: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(entries);
    let mut violations = Vec::new();
    crate::transaction_topic_uuid::verify(&scenario(), &index, &mut violations);
    violations
}

fn transaction_validation(entries: &mut [HistoryEntry]) -> &mut bool {
    let HistoryPayload::HarnessCommand { command } = &mut entries[4].payload else {
        panic!("transaction command")
    };
    let AdapterCommand::ExecuteTransaction {
        validate_topic_uuids,
        ..
    } = &mut command.command
    else {
        panic!("execute transaction command")
    };
    validate_topic_uuids
}

fn transaction_ids(entries: &mut [HistoryEntry]) -> &mut Vec<[u8; 16]> {
    let HistoryPayload::AdapterEvent { event } = &mut entries[9].payload else {
        panic!("transaction completion")
    };
    let AdapterEvent::TransactionCompleted {
        validated_topic_ids,
        ..
    } = &mut event.event
    else {
        panic!("transaction completion event")
    };
    validated_topic_ids
}

fn identity(entries: &mut [HistoryEntry], index: usize) -> &mut BrokerTopicIdentityState {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[2 + index].payload
    else {
        panic!("topic identity")
    };
    let BrokerStateObservation::TopicIdentity(value) = observation else {
        panic!("topic identity state")
    };
    value
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "TXN-010"),
        "{violations:?}"
    );
}

fn transaction_id() -> testlab_schema::OperationId {
    testlab_schema::OperationId::new("transaction-topic-uuid-commit")
        .unwrap_or_else(|error| panic!("transaction ID: {error}"))
}

fn identity_operation_id() -> testlab_schema::OperationId {
    testlab_schema::OperationId::new("transaction-topic-identities")
        .unwrap_or_else(|error| panic!("identity operation ID: {error}"))
}

fn operation_ids() -> [testlab_schema::OperationId; 2] {
    ["op-topic-uuid-alpha", "op-topic-uuid-zulu"].map(|value| {
        testlab_schema::OperationId::new(value)
            .unwrap_or_else(|error| panic!("operation ID: {error}"))
    })
}

const fn topics() -> [&'static str; 2] {
    [
        "testlab-transaction-topic-uuid-alpha",
        "testlab-transaction-topic-uuid-zulu",
    ]
}

fn id(index: usize) -> [u8; 16] {
    [u8::try_from(index + 1).unwrap_or_else(|error| panic!("topic ID: {error}")); 16]
}
