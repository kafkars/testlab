//! Offset-result normalization tests enforce one exact topic-partition identity.

use crate::kafkars_api::{
    ErrorKind, KafkaError, OffsetSpec, ReadIsolation, StartPosition, TopicPartition,
};
use testlab_schema::{AdminOffsetSelector, AdminReadIsolation, OperationId, ROUTING_ERROR_CODE};

use crate::AdapterError;
use crate::normalize::error_code;
use crate::protocol_admin_read::{offset_spec, public_read_isolation};
use crate::protocol_admin_result::listed_offset;

#[test]
fn offset_positions_map_to_exact_public_specs() {
    assert_eq!(
        offset_spec(AdminOffsetSelector::Earliest, None),
        Some(OffsetSpec::earliest())
    );
    assert_eq!(
        offset_spec(AdminOffsetSelector::Latest, None),
        Some(OffsetSpec::latest())
    );
    assert_eq!(
        offset_spec(AdminOffsetSelector::MaxTimestamp, None),
        Some(OffsetSpec::max_timestamp())
    );
    assert_eq!(
        offset_spec(AdminOffsetSelector::Timestamp, Some(1_700_000_000_123)),
        Some(OffsetSpec::for_timestamp(1_700_000_000_123))
    );
    assert_eq!(offset_spec(AdminOffsetSelector::Timestamp, None), None);
    assert_eq!(offset_spec(AdminOffsetSelector::Timestamp, Some(-1)), None);
    assert_eq!(
        offset_spec(AdminOffsetSelector::MaxTimestamp, Some(1)),
        None
    );
    assert_eq!(offset_spec(AdminOffsetSelector::Latest, Some(1)), None);
}

#[test]
fn admin_read_isolation_maps_both_public_selections() {
    assert_eq!(
        public_read_isolation(AdminReadIsolation::ReadCommitted),
        ReadIsolation::ReadCommitted
    );
    assert_eq!(
        public_read_isolation(AdminReadIsolation::ReadUncommitted),
        ReadIsolation::ReadUncommitted
    );
}

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
fn listed_offset_preserves_and_normalizes_routing_failure() {
    let result = listed_offset(
        vec![entry(
            "orders",
            2,
            Err(KafkaError::new(
                ErrorKind::Routing,
                "partition is unroutable",
            )),
        )],
        &operation_id(),
        "orders",
        2,
    );

    let Err(AdapterError::Client(error)) = result else {
        panic!("routing failure must remain a client failure");
    };
    assert_eq!(error.kind(), ErrorKind::Routing);
    assert_eq!(error_code(&error), ROUTING_ERROR_CODE);
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
