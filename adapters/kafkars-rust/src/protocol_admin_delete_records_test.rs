//! Delete-records normalization tests enforce one exact public batch identity.

use crate::kafkars_api::{ErrorKind, KafkaError, StartPosition, TopicPartition};
use testlab_schema::OperationId;

use crate::AdapterError;
use crate::protocol_admin_result::deleted_records_low_watermark;

#[test]
fn delete_records_preserves_reported_low_watermark() {
    let result = deleted_records_low_watermark(
        vec![entry("orders", 2, Ok(41))],
        &operation_id(),
        "orders",
        2,
    );

    assert_eq!(
        result.unwrap_or_else(|error| panic!("delete-records result: {error}")),
        41
    );
}

#[test]
fn delete_records_rejects_malformed_and_mismatched_results() {
    let operation_id = operation_id();
    let empty = deleted_records_low_watermark(Vec::new(), &operation_id, "orders", 2);
    let extra = deleted_records_low_watermark(
        vec![entry("orders", 2, Ok(41)), entry("orders", 3, Ok(12))],
        &operation_id,
        "orders",
        2,
    );
    let wrong_topic =
        deleted_records_low_watermark(vec![entry("audit", 2, Ok(41))], &operation_id, "orders", 2);
    let wrong_partition =
        deleted_records_low_watermark(vec![entry("orders", 1, Ok(41))], &operation_id, "orders", 2);
    let positioned = deleted_records_low_watermark(
        vec![(
            TopicPartition::new("orders", 2).start_at(StartPosition::Beginning),
            Ok(41),
        )],
        &operation_id,
        "orders",
        2,
    );

    for result in [empty, extra, wrong_topic, wrong_partition, positioned] {
        assert!(matches!(result, Err(AdapterError::AdminResult(_))));
    }
}

#[test]
fn delete_records_preserves_per_partition_client_failure() {
    let result = deleted_records_low_watermark(
        vec![entry(
            "orders",
            2,
            Err(KafkaError::new(ErrorKind::Broker, "record deletion failed")),
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
    result: Result<i64, KafkaError>,
) -> (TopicPartition, Result<i64, KafkaError>) {
    (TopicPartition::new(topic, partition), result)
}

fn operation_id() -> OperationId {
    OperationId::new("admin-delete-records-1")
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}
