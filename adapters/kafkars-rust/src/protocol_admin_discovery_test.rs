//! Topic discovery normalization tests preserve public ordering and failures.

use kafkars::{ErrorKind, KafkaError};
use testlab_schema::OperationId;

use crate::AdapterError;
use crate::protocol_admin_result::{DescribedTopicResult, described_partitions, listed_topics};

#[test]
fn described_topic_preserves_public_partition_order() {
    let result = described_partitions(
        vec![(
            "orders".to_owned(),
            Ok(description("orders", vec![(0, None), (1, None), (2, None)])),
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

    for result in [empty, extra, wrong_key, wrong_name] {
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
                "__consumer_offsets".to_owned(),
                Ok("__consumer_offsets".to_owned()),
            ),
            ("audit".to_owned(), Ok("audit".to_owned())),
            ("orders".to_owned(), Ok("orders".to_owned())),
        ],
        &operation_id(),
    );
    let empty = listed_topics(Vec::new(), &operation_id());

    let topics = listed.unwrap_or_else(|error| panic!("list topics: {error}"));
    let empty_topics = empty.unwrap_or_else(|error| panic!("list empty topics: {error}"));
    assert_eq!(
        topics,
        vec![
            "__consumer_offsets".to_owned(),
            "audit".to_owned(),
            "orders".to_owned(),
        ]
    );
    assert!(empty_topics.is_empty());
}

#[test]
fn listed_topics_reject_name_mismatch_and_preserve_client_failure() {
    let mismatch = listed_topics(
        vec![("orders".to_owned(), Ok("audit".to_owned()))],
        &operation_id(),
    );
    let client_failure = listed_topics(
        vec![("orders".to_owned(), Err(client_error("listing failed")))],
        &operation_id(),
    );

    assert!(matches!(mismatch, Err(AdapterError::AdminResult(_))));
    assert!(matches!(client_failure, Err(AdapterError::Client(_))));
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
