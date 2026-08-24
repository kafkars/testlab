//! Group-offset observer tests prove issued targeting and strict offset normalization.

use std::collections::BTreeSet;

use rdkafka::error::KafkaError;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::types::RDKafkaErrorCode;
use testlab_schema::{
    BrokerStateObservation, ClientId, ListConsumerGroupOffsetsAction, OperationId, Scenario,
    ScenarioAction, ScenarioId, ScenarioStep, StepId,
};

use crate::observer_error::ObserverError;
use crate::observer_group_offset::{ConsumerGroupOffsetTarget, normalize_response, targets};

#[test]
fn unissued_group_offset_query_is_excluded() {
    assert!(targets(&scenario(), &[]).is_empty());
}

#[test]
fn issued_group_offset_query_retains_exact_identity() {
    let operation_id = id(OperationId::new("admin-group-offsets-1"));
    let issued = [command()];

    assert_eq!(
        targets(&scenario(), &issued),
        vec![ConsumerGroupOffsetTarget {
            operation_id,
            group_id: "testlab-classic-group".to_owned(),
            topic: "records".to_owned(),
            partition: 0,
        }]
    );
}

#[test]
fn same_identity_with_any_wrong_payload_is_excluded() {
    let mut wrong_client = command();
    wrong_client.client_id = id(ClientId::new("client-2"));
    let mut wrong_group = command();
    wrong_group.group_id = "other-group".to_owned();
    let mut wrong_topic = command();
    wrong_topic.topic = "other-topic".to_owned();
    let mut wrong_partition = command();
    wrong_partition.partition = 1;
    let mut wrong_stability = command();
    wrong_stability.require_stable = false;
    let mut wrong_timeout = command();
    wrong_timeout.timeout_ms = 501;

    for issued in [
        wrong_client,
        wrong_group,
        wrong_topic,
        wrong_partition,
        wrong_stability,
        wrong_timeout,
    ] {
        assert!(targets(&scenario(), &[issued]).is_empty());
    }
}

#[test]
fn duplicate_issued_identity_is_excluded() {
    assert!(targets(&scenario(), &[command(), command()]).is_empty());
}

#[test]
fn invalid_librdkafka_offset_is_preserved_as_absent() {
    let observed = normalize_response(3, &target(), &response("records", 0, Offset::Invalid))
        .unwrap_or_else(|error| panic!("normalize absent group offset: {error}"));

    assert_eq!(offset(&observed), None);
    assert_eq!(operation(&observed), "admin-group-offsets-1");
}

#[test]
fn concrete_librdkafka_offset_is_preserved() {
    let observed = normalize_response(0, &target(), &response("records", 0, Offset::Offset(1)))
        .unwrap_or_else(|error| panic!("normalize committed group offset: {error}"));

    assert_eq!(offset(&observed), Some(1));
}

#[test]
fn symbolic_or_mismatched_offsets_are_invalid() {
    assert!(normalize_response(0, &target(), &response("records", 0, Offset::Beginning)).is_err());
    assert!(normalize_response(0, &target(), &response("other", 0, Offset::Offset(1))).is_err());
    assert!(normalize_response(0, &target(), &TopicPartitionList::new()).is_err());
}

#[test]
fn offset_fetch_timeout_is_classified_as_an_observer_timeout() {
    let timeout = ObserverError::Kafka(KafkaError::MetadataFetch(
        RDKafkaErrorCode::OperationTimedOut,
    ));
    let error = ObserverError::Kafka(KafkaError::MetadataFetch(
        RDKafkaErrorCode::UnknownTopicOrPartition,
    ));

    assert!(timeout.is_timeout());
    assert!(!error.is_timeout());
}

fn scenario() -> Scenario {
    Scenario {
        schema_version: testlab_schema::SCENARIO_SCHEMA_VERSION,
        id: id(ScenarioId::new("kafka.admin-list-consumer-group-offsets")),
        title: "group offsets".to_owned(),
        description: "observer targeting fixture".to_owned(),
        timeout_ms: 1_000,
        requires: BTreeSet::new(),
        steps: vec![ScenarioStep {
            id: id(StepId::new("list-group-offset")),
            action: ScenarioAction::ListConsumerGroupOffsets(ListConsumerGroupOffsetsAction {
                client_id: id(ClientId::new("client-1")),
                operation_id: id(OperationId::new("admin-group-offsets-1")),
                group_id: "testlab-classic-group".to_owned(),
                topic: "records".to_owned(),
                partition: 0,
                require_stable: true,
                expected_offset: 1,
                timeout_ms: 500,
            }),
        }],
        assertions: Vec::new(),
    }
}

fn target() -> ConsumerGroupOffsetTarget {
    ConsumerGroupOffsetTarget {
        operation_id: id(OperationId::new("admin-group-offsets-1")),
        group_id: "testlab-classic-group".to_owned(),
        topic: "records".to_owned(),
        partition: 0,
    }
}

fn command() -> testlab_schema::ListConsumerGroupOffsetsCommand {
    testlab_schema::ListConsumerGroupOffsetsCommand {
        client_id: id(ClientId::new("client-1")),
        operation_id: id(OperationId::new("admin-group-offsets-1")),
        group_id: "testlab-classic-group".to_owned(),
        topic: "records".to_owned(),
        partition: 0,
        require_stable: true,
        timeout_ms: 500,
    }
}

fn response(topic: &str, partition: i32, offset: Offset) -> TopicPartitionList {
    let mut response = TopicPartitionList::new();
    id(response.add_partition_offset(topic, partition, offset));
    response
}

fn offset(observation: &BrokerStateObservation) -> Option<i64> {
    match observation {
        BrokerStateObservation::ConsumerGroupOffset { offset, .. } => *offset,
    }
}

fn operation(observation: &BrokerStateObservation) -> &str {
    match observation {
        BrokerStateObservation::ConsumerGroupOffset { operation_id, .. } => operation_id.as_str(),
    }
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture value: {error}"))
}
