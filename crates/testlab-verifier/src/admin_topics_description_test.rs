//! Plural topic descriptions require ordered detail and immediate ordered metadata.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTopicDescriptionOutcome, AdminTopicDescriptionValue,
    AdminTopicsDescription, BrokerStateObservation, BrokerTopicState, ClientId,
    DescribeTopicsCommand, HistoryEntry, HistoryPayload, OperationId, Scenario, TopicSelection,
    UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn exact_ordered_public_outcomes_and_metadata_pass() {
    assert!(violations(&history()).is_empty());
}

#[test]
fn reordered_or_duplicated_public_outcomes_fail() {
    let mut reordered = history();
    completion(&mut reordered).outcomes.swap(0, 2);
    assert_contract(&violations(&reordered));

    let mut duplicated = history();
    duplicated.insert(2, duplicated[1].clone());
    assert_contract(&violations(&duplicated));
}

#[test]
fn wrong_resource_shape_or_partition_detail_fails() {
    let mut wrong_error = history();
    completion(&mut wrong_error).outcomes[1].error_code = Some("broker:broker_29".to_owned());
    assert_contract(&violations(&wrong_error));

    let mut partition_error = history();
    description(&mut partition_error, 0).partitions[1].error_code =
        Some("broker:broker_3".to_owned());
    assert_contract(&violations(&partition_error));

    let mut lossy = history();
    description(&mut lossy, 0).partitions.pop();
    assert_contract(&violations(&lossy));
}

#[test]
fn missing_or_zero_public_topic_identity_fails() {
    let mut missing = history();
    description(&mut missing, 0).topic_id = None;
    assert_contract(&violations(&missing));

    let mut zero = history();
    description(&mut zero, 2).topic_id = Some([0; 16]);
    assert_contract(&violations(&zero));
}

#[test]
fn reordered_mismatched_or_noncontiguous_metadata_fails() {
    let mut reordered = history();
    reordered.swap(2, 3);
    assert_contract(&violations(&reordered));

    let mut mismatched = history();
    observation(&mut mismatched, 1).exists = true;
    assert_contract(&violations(&mismatched));

    let mut noncontiguous = history();
    observation(&mut noncontiguous, 2).observation = 44;
    assert_contract(&violations(&noncontiguous));
}

#[test]
fn altered_wire_topic_order_fails_exact_command_ownership() {
    let mut entries = history();
    let HistoryPayload::HarnessCommand { command } = &mut entries[0].payload else {
        panic!("plural topic-description command");
    };
    let AdapterCommand::DescribeTopics(value) = &mut command.command else {
        panic!("plural topic-description payload");
    };
    value.topics.swap(0, 1);
    assert_contract(&violations(&entries));
}

#[test]
fn missing_requested_authorization_or_changed_wire_flag_fails() {
    let mut missing = history();
    description(&mut missing, 0).authorized_operations = None;
    assert_contract(&violations(&missing));

    let mut changed = history();
    let HistoryPayload::HarnessCommand { command } = &mut changed[0].payload else {
        panic!("plural topic-description command");
    };
    let AdapterCommand::DescribeTopics(value) = &mut command.command else {
        panic!("plural topic-description payload");
    };
    value.include_authorized_operations = false;
    assert_contract(&violations(&changed));
}

fn history() -> Vec<HistoryEntry> {
    vec![
        command(1, AdapterCommand::DescribeTopics(command_payload())),
        event(2, AdapterEvent::TopicsDescribed(completion_value())),
        state(3, 40, zulu_topic(), true, vec![0, 1]),
        state(4, 41, missing_topic(), false, Vec::new()),
        state(5, 42, alpha_topic(), true, vec![0]),
    ]
}

fn command_payload() -> DescribeTopicsCommand {
    DescribeTopicsCommand {
        client_id: client(),
        operation_id: operation(),
        selection: TopicSelection::Name,
        topics: topic_names(),
        include_authorized_operations: true,
        timeout_ms: 20_000,
    }
}

fn completion_value() -> AdminTopicsDescription {
    AdminTopicsDescription {
        operation_id: operation(),
        outcomes: vec![
            successful(zulu_topic(), vec![0, 1], 1),
            AdminTopicDescriptionOutcome {
                topic: missing_topic().to_owned(),
                topic_id: None,
                description: None,
                error_code: Some(UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE.to_owned()),
            },
            successful(alpha_topic(), vec![0], 2),
        ],
    }
}

fn successful(topic: &str, partitions: Vec<i32>, topic_id: u8) -> AdminTopicDescriptionOutcome {
    AdminTopicDescriptionOutcome {
        topic: topic.to_owned(),
        topic_id: None,
        description: Some(AdminTopicDescriptionValue {
            topic_id: Some([topic_id; 16]),
            internal: false,
            authorized_operations: Some(1),
            partitions: partitions
                .into_iter()
                .map(crate::admin_topic::topology::partition)
                .collect(),
        }),
        error_code: None,
    }
}

fn state(
    sequence: u64,
    observation: u64,
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
                operation_id: operation(),
                topic: topic.to_owned(),
                exists,
                partitions,
            }),
        },
    }
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-describe-topics.toml"
    ))
    .unwrap_or_else(|error| panic!("parse plural topic-description scenario: {error}"))
}

fn violations(entries: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(entries);
    let mut violations = Vec::new();
    verify_admin(&scenario(), &index, &[], &mut violations);
    violations
}

fn completion(entries: &mut [HistoryEntry]) -> &mut AdminTopicsDescription {
    let HistoryPayload::AdapterEvent { event } = &mut entries[1].payload else {
        panic!("plural topic-description event");
    };
    let AdapterEvent::TopicsDescribed(value) = &mut event.event else {
        panic!("plural topic-description completion");
    };
    value
}

fn description(entries: &mut [HistoryEntry], index: usize) -> &mut AdminTopicDescriptionValue {
    completion(entries).outcomes[index]
        .description
        .as_mut()
        .unwrap_or_else(|| panic!("successful topic description"))
}

fn observation(entries: &mut [HistoryEntry], index: usize) -> &mut BrokerTopicState {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[2 + index].payload
    else {
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
            .any(|violation| violation.contract_id.as_str() == "ADMIN-044"),
        "{violations:?}"
    );
}

fn topic_names() -> Vec<String> {
    vec![
        zulu_topic().to_owned(),
        missing_topic().to_owned(),
        alpha_topic().to_owned(),
    ]
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-describe-topics").unwrap_or_else(|error| panic!("operation: {error}"))
}

fn zulu_topic() -> &'static str {
    "testlab-kafkars-admin-describe-topics-zulu"
}

fn missing_topic() -> &'static str {
    "testlab-kafkars-admin-describe-topics-missing"
}

fn alpha_topic() -> &'static str {
    "testlab-kafkars-admin-describe-topics-alpha"
}
