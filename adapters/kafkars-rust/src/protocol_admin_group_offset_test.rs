//! Group-offset normalization tests enforce one exact public batch identity.

use crate::kafkars_api::{ErrorKind, KafkaError, StartPosition, TopicPartition};
use testlab_schema::OperationId;

use crate::AdapterError;
use crate::protocol_admin_read::stable_offset_broker_retryable;
use crate::protocol_admin_result::listed_consumer_group_offset;

#[test]
fn group_offset_preserves_present_and_absent_committed_offsets() {
    let present = listed_consumer_group_offset(
        vec![entry("orders", 2, Ok(Some(41)))],
        &operation_id(),
        "orders",
        2,
    );
    let absent = listed_consumer_group_offset(
        vec![entry("orders", 2, Ok(None))],
        &operation_id(),
        "orders",
        2,
    );

    assert_eq!(
        present.unwrap_or_else(|error| panic!("present: {error}")),
        Some(41)
    );
    assert_eq!(
        absent.unwrap_or_else(|error| panic!("absent: {error}")),
        None
    );
}

#[test]
fn group_offset_rejects_malformed_batch_shapes() {
    let operation_id = operation_id();
    let empty = listed_consumer_group_offset(Vec::new(), &operation_id, "orders", 2);
    let duplicate = listed_consumer_group_offset(
        vec![
            entry("orders", 2, Ok(Some(1))),
            entry("orders", 2, Ok(Some(1))),
        ],
        &operation_id,
        "orders",
        2,
    );
    let wrong_topic = listed_consumer_group_offset(
        vec![entry("audit", 2, Ok(Some(1)))],
        &operation_id,
        "orders",
        2,
    );
    let wrong_partition = listed_consumer_group_offset(
        vec![entry("orders", 1, Ok(Some(1)))],
        &operation_id,
        "orders",
        2,
    );
    let positioned = listed_consumer_group_offset(
        vec![(
            TopicPartition::new("orders", 2).start_at(StartPosition::Beginning),
            Ok(Some(1)),
        )],
        &operation_id,
        "orders",
        2,
    );

    for result in [empty, duplicate, wrong_topic, wrong_partition, positioned] {
        assert!(matches!(result, Err(AdapterError::AdminResult(_))));
    }
}

#[test]
fn group_offset_preserves_per_partition_client_failure() {
    let result = listed_consumer_group_offset(
        vec![entry(
            "orders",
            2,
            Err(KafkaError::new(ErrorKind::Broker, "offset fetch failed")),
        )],
        &operation_id(),
        "orders",
        2,
    );

    assert!(matches!(result, Err(AdapterError::Client(_))));
}

#[test]
fn only_stable_offset_code_88_is_polled_under_the_original_deadline() {
    assert!(stable_offset_broker_retryable(
        true,
        ErrorKind::Broker,
        Some(88)
    ));
    assert!(!stable_offset_broker_retryable(
        false,
        ErrorKind::Broker,
        Some(88)
    ));
    assert!(!stable_offset_broker_retryable(
        true,
        ErrorKind::Broker,
        Some(14)
    ));
    assert!(!stable_offset_broker_retryable(
        true,
        ErrorKind::Timeout,
        Some(88)
    ));
}

fn entry(
    topic: &str,
    partition: i32,
    result: Result<Option<i64>, KafkaError>,
) -> (TopicPartition, Result<Option<i64>, KafkaError>) {
    (TopicPartition::new(topic, partition), result)
}

fn operation_id() -> OperationId {
    OperationId::new("admin-group-offset-1").unwrap_or_else(|error| panic!("operation id: {error}"))
}
