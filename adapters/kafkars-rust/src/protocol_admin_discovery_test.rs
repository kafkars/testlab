//! Topic discovery normalization tests preserve public ordering and failures.

use crate::kafkars_api::{ErrorKind, KafkaError};
use testlab_schema::OperationId;

use crate::AdapterError;
use crate::protocol_admin_result::{DescribedTopicResult, described_partitions};
use crate::protocol_admin_topic_listing_result::{ListedTopicResult, listed_topics};

#[test]
fn described_topic_canonicalizes_public_partition_order() {
    let result = described_partitions(
        vec![(
            "orders".to_owned(),
            Ok(description("orders", vec![(2, None), (0, None), (1, None)])),
        )],
        &operation_id(),
        "orders",
    );

    let partitions = result.unwrap_or_else(|error| panic!("describe topic: {error}"));
    assert_eq!(partitions, vec![0, 1, 2]);
}

#[test]
fn described_topic_rejects_malformed_batch_and_names() {
    let operation_id = operation_id();
    let empty = described_partitions(Vec::new(), &operation_id, "orders");
    let extra = described_partitions(
        vec![
            ("orders".to_owned(), Ok(description("orders", Vec::new()))),
            ("audit".to_owned(), Ok(description("audit", Vec::new()))),
        ],
        &operation_id,
        "orders",
    );
    let wrong_key = described_partitions(
        vec![("audit".to_owned(), Ok(description("audit", Vec::new())))],
        &operation_id,
        "orders",
    );
    let wrong_name = described_partitions(
        vec![("orders".to_owned(), Ok(description("audit", Vec::new())))],
        &operation_id,
        "orders",
    );
    let duplicate = described_partitions(
        vec![(
            "orders".to_owned(),
            Ok(description("orders", vec![(0, None), (0, None)])),
        )],
        &operation_id,
        "orders",
    );
    let negative = described_partitions(
        vec![(
            "orders".to_owned(),
            Ok(description("orders", vec![(-1, None)])),
        )],
        &operation_id,
        "orders",
    );

    for result in [empty, extra, wrong_key, wrong_name, duplicate, negative] {
        assert!(matches!(result, Err(AdapterError::AdminResult(_))));
    }
}

#[test]
fn described_topic_preserves_topic_and_partition_client_failures() {
    let operation_id = operation_id();
    let topic_failure = described_partitions(
        vec![("orders".to_owned(), Err(client_error("topic failed")))],
        &operation_id,
        "orders",
    );
    let partition_failure = described_partitions(
        vec![(
            "orders".to_owned(),
            Ok(description(
                "orders",
                vec![(0, Some(client_error("partition failed")))],
            )),
        )],
        &operation_id,
        "orders",
    );

    assert!(matches!(topic_failure, Err(AdapterError::Client(_))));
    assert!(matches!(partition_failure, Err(AdapterError::Client(_))));
}

#[test]
fn listed_topics_preserve_public_byte_order_and_allow_empty_results() {
    let listed = listed_topics(
        vec![
            (
                "orders".to_owned(),
                Ok(listed_description("orders", Some(7))),
            ),
            ("audit".to_owned(), Ok(listed_description("audit", Some(7)))),
            (
                "__consumer_offsets".to_owned(),
                Ok(listed_description("__consumer_offsets", Some(7))),
            ),
        ],
        &operation_id(),
    );
    let empty = listed_topics(Vec::new(), &operation_id());

    let topics = listed.unwrap_or_else(|error| panic!("list topics: {error}"));
    let empty_topics = empty.unwrap_or_else(|error| panic!("list empty topics: {error}"));
    assert_eq!(
        topics
            .iter()
            .map(|outcome| outcome.topic.as_str())
            .collect::<Vec<_>>(),
        vec!["__consumer_offsets", "audit", "orders",]
    );
    assert!(topics.iter().all(|outcome| {
        outcome.description.as_ref().is_some_and(|description| {
            description.authorized_operations == Some(7) && description.partitions[0].partition == 0
        }) && outcome.error_code.is_none()
    }));
    assert!(empty_topics.is_empty());
}

#[test]
fn listed_topics_reject_name_mismatch_and_preserve_resource_failure() {
    let mismatch = listed_topics(
        vec![("orders".to_owned(), Ok(listed_description("audit", None)))],
        &operation_id(),
    );
    let resource_failure = listed_topics(
        vec![("orders".to_owned(), Err(client_error("listing failed")))],
        &operation_id(),
    )
    .unwrap_or_else(|error| panic!("retain listing failure: {error}"));
    let duplicate = listed_topics(
        vec![
            ("orders".to_owned(), Ok(listed_description("orders", None))),
            ("orders".to_owned(), Ok(listed_description("orders", None))),
        ],
        &operation_id(),
    );

    assert!(matches!(mismatch, Err(AdapterError::AdminResult(_))));
    assert!(matches!(duplicate, Err(AdapterError::AdminResult(_))));
    assert_eq!(resource_failure[0].topic, "orders");
    assert!(resource_failure[0].description.is_none());
    assert!(resource_failure[0].error_code.is_some());
}

fn listed_description(name: &str, authorized_operations: Option<i32>) -> ListedTopicResult {
    ListedTopicResult {
        name: name.to_owned(),
        topic_id: Some([1; 16]),
        internal: name.starts_with("__"),
        authorized_operations,
        partitions: vec![(0, None)],
    }
}

fn description(name: &str, partitions: Vec<(i32, Option<KafkaError>)>) -> DescribedTopicResult {
    DescribedTopicResult {
        name: name.to_owned(),
        partitions,
    }
}

fn client_error(message: &str) -> KafkaError {
    KafkaError::new(ErrorKind::Broker, message)
}

fn operation_id() -> OperationId {
    OperationId::new("admin-discovery-1").unwrap_or_else(|error| panic!("operation id: {error}"))
}
