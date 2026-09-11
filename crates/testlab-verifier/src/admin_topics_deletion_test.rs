//! Plural topic deletion requires an ordered baseline, public result, and final absence.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTopicDeletionOutcome, AdminTopicDescriptionOutcome,
    AdminTopicDescriptionValue, AdminTopicPartitionDescriptionOutcome, AdminTopicsDeletion,
    AdminTopicsDescription, BrokerStateObservation, BrokerTopicState, ClientId,
    DeleteTopicsCommand, DescribeTopicsCommand, HistoryEntry, HistoryPayload, OperationId,
    Scenario, TopicSelection, UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn exact_baseline_ordered_outcomes_and_absence_pass() {
    assert!(violations(&history()).is_empty());
}

#[test]
fn reordered_or_wrong_public_outcomes_fail() {
    let mut reordered = history();
    deletion(&mut reordered).outcomes.swap(0, 2);
    assert_contract(&violations(&reordered));
    let mut wrong = history();
    deletion(&mut wrong).outcomes[1].error_code = Some("broker:broker_29".to_owned());
    assert_contract(&violations(&wrong));
}

#[test]
fn absent_success_or_present_missing_baseline_fails() {
    let mut absent = history();
    baseline(&mut absent, 0).exists = false;
    baseline(&mut absent, 0).partitions.clear();
    assert_contract(&violations(&absent));
    let mut present = history();
    baseline(&mut present, 1).exists = true;
    baseline(&mut present, 1).partitions.push(0);
    assert_contract(&violations(&present));
}

#[test]
fn present_reordered_or_noncontiguous_final_state_fails() {
    let mut present = history();
    final_state(&mut present, 0).exists = true;
    final_state(&mut present, 0).partitions.push(0);
    assert_contract(&violations(&present));
    let mut reordered = history();
    final_state(&mut reordered, 0).topic = alpha().to_owned();
    assert_contract(&violations(&reordered));
    let mut noncontiguous = history();
    final_state(&mut noncontiguous, 2).observation = 77;
    assert_contract(&violations(&noncontiguous));
}

#[test]
fn altered_wire_order_fails_exact_command_ownership() {
    let mut entries = history();
    let HistoryPayload::HarnessCommand { command } = &mut entries[5].payload else {
        panic!("plural topic-deletion command");
    };
    let AdapterCommand::DeleteTopics(value) = &mut command.command else {
        panic!("plural topic-deletion payload");
    };
    value.topics.swap(0, 1);
    assert_contract(&violations(&entries));
}

fn history() -> Vec<HistoryEntry> {
    vec![
        command(1, AdapterCommand::DescribeTopics(description_command())),
        event(2, AdapterEvent::TopicsDescribed(description())),
        state(3, 40, description_operation(), zulu(), true, vec![0, 1]),
        state(4, 41, description_operation(), missing(), false, Vec::new()),
        state(5, 42, description_operation(), alpha(), true, vec![0]),
        command(6, AdapterCommand::DeleteTopics(deletion_command())),
        event(7, AdapterEvent::TopicsDeleted(deletion_value())),
        state(8, 43, deletion_operation(), zulu(), false, Vec::new()),
        state(9, 44, deletion_operation(), missing(), false, Vec::new()),
        state(10, 45, deletion_operation(), alpha(), false, Vec::new()),
    ]
}

fn description_command() -> DescribeTopicsCommand {
    DescribeTopicsCommand {
        client_id: client(),
        operation_id: description_operation(),
        selection: TopicSelection::Name,
        topics: names(),
        timeout_ms: 20_000,
    }
}

fn deletion_command() -> DeleteTopicsCommand {
    DeleteTopicsCommand {
        client_id: client(),
        operation_id: deletion_operation(),
        selection: TopicSelection::Name,
        topics: names(),
        timeout_ms: 30_000,
    }
}

fn description() -> AdminTopicsDescription {
    AdminTopicsDescription {
        operation_id: description_operation(),
        outcomes: vec![
            described(zulu(), vec![0, 1], 1),
            AdminTopicDescriptionOutcome {
                topic: missing().to_owned(),
                topic_id: None,
                description: None,
                error_code: Some(UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE.to_owned()),
            },
            described(alpha(), vec![0], 2),
        ],
    }
}

fn described(topic: &str, partitions: Vec<i32>, id: u8) -> AdminTopicDescriptionOutcome {
    AdminTopicDescriptionOutcome {
        topic: topic.to_owned(),
        topic_id: None,
        description: Some(AdminTopicDescriptionValue {
            topic_id: Some([id; 16]),
            internal: false,
            partitions: partitions
                .into_iter()
                .map(|partition| AdminTopicPartitionDescriptionOutcome {
                    partition,
                    error_code: None,
                })
                .collect(),
        }),
        error_code: None,
    }
}

fn deletion_value() -> AdminTopicsDeletion {
    AdminTopicsDeletion {
        operation_id: deletion_operation(),
        outcomes: vec![
            deleted(zulu(), None),
            deleted(missing(), Some(UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE)),
            deleted(alpha(), None),
        ],
    }
}

fn deleted(topic: &str, error: Option<&str>) -> AdminTopicDeletionOutcome {
    AdminTopicDeletionOutcome {
        topic: topic.to_owned(),
        topic_id: None,
        error_code: error.map(str::to_owned),
    }
}

fn state(
    sequence: u64,
    observation: u64,
    operation_id: OperationId,
    topic: &str,
    exists: bool,
    partitions: Vec<i32>,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::Topic(BrokerTopicState {
                observation,
                operation_id,
                topic: topic.to_owned(),
                exists,
                partitions,
            }),
        },
    }
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-delete-topics.toml"
    ))
    .unwrap_or_else(|error| panic!("parse plural topic-deletion scenario: {error}"))
}

fn violations(entries: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(entries);
    let mut violations = Vec::new();
    verify_admin(&scenario(), &index, &[], &mut violations);
    violations
}

fn deletion(entries: &mut [HistoryEntry]) -> &mut AdminTopicsDeletion {
    let HistoryPayload::AdapterEvent { event } = &mut entries[6].payload else {
        panic!("plural topic-deletion event");
    };
    let AdapterEvent::TopicsDeleted(value) = &mut event.event else {
        panic!("plural topic-deletion completion");
    };
    value
}

fn baseline(entries: &mut [HistoryEntry], index: usize) -> &mut BrokerTopicState {
    topic_state(&mut entries[2 + index])
}

fn final_state(entries: &mut [HistoryEntry], index: usize) -> &mut BrokerTopicState {
    topic_state(&mut entries[7 + index])
}

fn topic_state(entry: &mut HistoryEntry) -> &mut BrokerTopicState {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entry.payload else {
        panic!("topic observation");
    };
    let BrokerStateObservation::Topic(value) = observation else {
        panic!("topic state");
    };
    value
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-045"),
        "{violations:?}"
    );
}

fn names() -> Vec<String> {
    vec![zulu().to_owned(), missing().to_owned(), alpha().to_owned()]
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn description_operation() -> OperationId {
    OperationId::new("admin-describe-topics-before-delete")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}

fn deletion_operation() -> OperationId {
    OperationId::new("admin-delete-topics").unwrap_or_else(|error| panic!("operation: {error}"))
}

fn zulu() -> &'static str {
    "testlab-kafkars-admin-delete-topics-zulu"
}

fn missing() -> &'static str {
    "testlab-kafkars-admin-delete-topics-missing"
}

fn alpha() -> &'static str {
    "testlab-kafkars-admin-delete-topics-alpha"
}
