//! Offset-result normalization tests enforce one exact topic-partition identity.

use kafkars::{ErrorKind, KafkaError, StartPosition, TopicPartition};
use testlab_schema::OperationId;

use crate::AdapterError;
use crate::protocol_admin_result::listed_offset;

#[test]
fn listed_offset_preserves_present_and_absent_public_offsets() {
    let present = listed_offset(
        vec![entry("orders", 2, Ok(Some(41)))],
        &operation_id(),
        "orders",
        2,
    );
    let absent = listed_offset(
        vec![entry("orders", 2, Ok(None))],
        &operation_id(),
        "orders",
        2,
    );

    let present = present.unwrap_or_else(|error| panic!("list present offset: {error}"));
    let absent = absent.unwrap_or_else(|error| panic!("list absent offset: {error}"));
    assert_eq!(present, Some(41));
    assert_eq!(absent, None);
}

#[test]
fn listed_offset_rejects_empty_extra_and_mismatched_results() {
    let operation_id = operation_id();
    let empty = listed_offset(Vec::new(), &operation_id, "orders", 2);
    let extra = listed_offset(
        vec![
            entry("orders", 2, Ok(Some(1))),
            entry("orders", 3, Ok(Some(2))),
        ],
        &operation_id,
        "orders",
        2,
    );
    let wrong_topic = listed_offset(
        vec![entry("audit", 2, Ok(Some(1)))],
        &operation_id,
        "orders",
        2,
    );
    let wrong_partition = listed_offset(
        vec![entry("orders", 1, Ok(Some(1)))],
        &operation_id,
        "orders",
        2,
    );
    let positioned_key = listed_offset(
        vec![(
            TopicPartition::new("orders", 2).start_at(StartPosition::Beginning),
            Ok(Some(1)),
        )],
        &operation_id,
        "orders",
        2,
    );

    for result in [empty, extra, wrong_topic, wrong_partition, positioned_key] {
        assert!(matches!(result, Err(AdapterError::AdminResult(_))));
    }
}

#[test]
fn listed_offset_preserves_per_partition_client_failure() {
    let result = listed_offset(
        vec![entry(
            "orders",
            2,
            Err(KafkaError::new(ErrorKind::Broker, "offset lookup failed")),
        )],
        &operation_id(),
        "orders",
        2,
    );

    assert!(matches!(result, Err(AdapterError::Client(_))));
}

fn entry(
    topic: &str,
    partition: i32,
    result: Result<Option<i64>, KafkaError>,
) -> (TopicPartition, Result<Option<i64>, KafkaError>) {
    (TopicPartition::new(topic, partition), result)
}

fn operation_id() -> OperationId {
    OperationId::new("admin-offset-1").unwrap_or_else(|error| panic!("operation id: {error}"))
}
