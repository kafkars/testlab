//! Topic-ID admin contracts join public UUID keys to independent broker identities.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTopicDeletionOutcome, AdminTopicDescriptionOutcome,
    AdminTopicDescriptionValue, AdminTopicPartitionDescriptionOutcome, AdminTopicsDeletion,
    AdminTopicsDescription, BrokerStateObservation, BrokerTopicIdentityState, BrokerTopicState,
    ClientId, DeleteTopicsCommand, DescribeTopicsCommand, HistoryEntry, HistoryPayload,
    OperationId, Scenario, TopicSelection,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn exact_public_and_independent_topic_ids_pass() {
    assert!(violations(&history()).is_empty());
}

#[test]
fn mismatched_description_key_or_inner_identity_fails() {
    let mut request_key = history();
    described(&mut request_key, 0).topic_id = Some([9; 16]);
    assert_contract(&violations(&request_key), "ADMIN-061");

    let mut inner = history();
    described(&mut inner, 1)
        .description
        .as_mut()
        .unwrap_or_else(|| panic!("description"))
        .topic_id = Some([9; 16]);
    assert_contract(&violations(&inner), "ADMIN-061");
}

#[test]
fn deletion_must_use_independently_observed_ids_and_reach_absence() {
    let mut wrong_id = history();
    deletion(&mut wrong_id).outcomes[0].topic_id = Some([9; 16]);
    assert_contract(&violations(&wrong_id), "ADMIN-062");

    let mut present = history();
    final_state(&mut present, 1).exists = true;
    final_state(&mut present, 1).partitions.push(0);
    assert_contract(&violations(&present), "ADMIN-062");
}

fn history() -> Vec<HistoryEntry> {
    vec![
        command(1, AdapterCommand::DescribeTopics(description_command())),
        event(2, AdapterEvent::TopicsDescribed(description_value())),
        identity_state(3, 40, description_operation(), zulu(), [1; 16], vec![0, 1]),
        identity_state(4, 41, description_operation(), alpha(), [2; 16], vec![0]),
        command(5, AdapterCommand::DeleteTopics(deletion_command())),
        event(6, AdapterEvent::TopicsDeleted(deletion_value())),
        topic_state(7, 42, deletion_operation(), zulu(), false, Vec::new()),
        topic_state(8, 43, deletion_operation(), alpha(), false, Vec::new()),
    ]
}

fn description_command() -> DescribeTopicsCommand {
    DescribeTopicsCommand {
        client_id: client(),
        operation_id: description_operation(),
        selection: TopicSelection::TopicId,
        topics: names(),
        timeout_ms: 20_000,
    }
}

fn deletion_command() -> DeleteTopicsCommand {
    DeleteTopicsCommand {
        client_id: client(),
        operation_id: deletion_operation(),
        selection: TopicSelection::TopicId,
        topics: names(),
        timeout_ms: 30_000,
    }
}

fn description_value() -> AdminTopicsDescription {
    AdminTopicsDescription {
        operation_id: description_operation(),
        outcomes: vec![
            description(zulu(), [1; 16], vec![0, 1]),
            description(alpha(), [2; 16], vec![0]),
        ],
    }
}

fn description(
    topic: &str,
    topic_id: [u8; 16],
    partitions: Vec<i32>,
) -> AdminTopicDescriptionOutcome {
    AdminTopicDescriptionOutcome {
        topic: topic.to_owned(),
        topic_id: Some(topic_id),
        description: Some(AdminTopicDescriptionValue {
            topic_id: Some(topic_id),
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
            deletion_outcome(zulu(), [1; 16]),
            deletion_outcome(alpha(), [2; 16]),
        ],
    }
}

fn deletion_outcome(topic: &str, topic_id: [u8; 16]) -> AdminTopicDeletionOutcome {
    AdminTopicDeletionOutcome {
        topic: topic.to_owned(),
        topic_id: Some(topic_id),
        error_code: None,
    }
}

fn identity_state(
    sequence: u64,
    observation: u64,
    operation_id: OperationId,
    topic: &str,
    topic_id: [u8; 16],
    partitions: Vec<i32>,
) -> HistoryEntry {
    state(
        sequence,
        BrokerStateObservation::TopicIdentity(BrokerTopicIdentityState {
            observation,
            operation_id,
            topic: topic.to_owned(),
            topic_id,
            partitions,
        }),
    )
}

fn topic_state(
    sequence: u64,
    observation: u64,
    operation_id: OperationId,
    topic: &str,
    exists: bool,
    partitions: Vec<i32>,
) -> HistoryEntry {
    state(
        sequence,
        BrokerStateObservation::Topic(BrokerTopicState {
            observation,
            operation_id,
            topic: topic.to_owned(),
            exists,
            partitions,
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

fn described(entries: &mut [HistoryEntry], index: usize) -> &mut AdminTopicDescriptionOutcome {
    let HistoryPayload::AdapterEvent { event } = &mut entries[1].payload else {
        panic!("topic-ID description event");
    };
    let AdapterEvent::TopicsDescribed(value) = &mut event.event else {
        panic!("topic-ID descriptions");
    };
    &mut value.outcomes[index]
}

fn deletion(entries: &mut [HistoryEntry]) -> &mut AdminTopicsDeletion {
    let HistoryPayload::AdapterEvent { event } = &mut entries[5].payload else {
        panic!("topic-ID deletion event");
    };
    let AdapterEvent::TopicsDeleted(value) = &mut event.event else {
        panic!("topic-ID deletions");
    };
    value
}

fn final_state(entries: &mut [HistoryEntry], index: usize) -> &mut BrokerTopicState {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[6 + index].payload
    else {
        panic!("topic absence observation");
    };
    let BrokerStateObservation::Topic(value) = observation else {
        panic!("topic state");
    };
    value
}

fn violations(entries: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(entries);
    let mut violations = Vec::new();
    verify_admin(&scenario(), &index, &[], &mut violations);
    violations
}

fn assert_contract(violations: &[testlab_schema::Violation], contract: &str) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract),
        "{violations:?}"
    );
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-topic-ids.toml"
    ))
    .unwrap_or_else(|error| panic!("parse topic-ID scenario: {error}"))
}

fn names() -> Vec<String> {
    vec![zulu().to_owned(), alpha().to_owned()]
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn description_operation() -> OperationId {
    OperationId::new("admin-describe-topics-by-id")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}

fn deletion_operation() -> OperationId {
    OperationId::new("admin-delete-topics-by-id")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}

const fn zulu() -> &'static str {
    "testlab-kafkars-admin-topic-ids-zulu"
}

const fn alpha() -> &'static str {
    "testlab-kafkars-admin-topic-ids-alpha"
}
