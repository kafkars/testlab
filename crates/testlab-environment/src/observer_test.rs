//! Observer tests prove issued targeting, exact bytes, and correlation metadata.

use std::collections::BTreeSet;

use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;
use testlab_schema::{ByteEncoding, OperationId, Scenario};

use crate::observer::{is_transient, targets};
use crate::observer_record::{CapturedRecord, normalize};

#[test]
fn unissued_partition_send_is_excluded_from_observation_targets() {
    let scenario = partition_expansion_scenario();

    assert!(targets(&scenario, &BTreeSet::new()).is_empty());
}

#[test]
fn issued_partition_send_is_included_in_observation_targets() {
    let scenario = partition_expansion_scenario();
    let issued = BTreeSet::from([id(OperationId::new("op-expanded-partition"))]);

    assert_eq!(
        targets(&scenario, &issued),
        BTreeSet::from([("testlab-kafkars-admin-partitions".to_owned(), 2)])
    );
}

#[test]
fn issued_concurrent_sends_are_independent_observation_targets() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/concurrent-multi-producer.toml"
    ))
    .unwrap_or_else(|error| panic!("parse concurrent scenario: {error}"));
    let issued = ["op-a-0", "op-b-1"]
        .into_iter()
        .map(|value| id(OperationId::new(value)))
        .collect();

    assert_eq!(
        targets(&scenario, &issued),
        BTreeSet::from([
            ("testlab-kafkars-concurrent-multi-producer".to_owned(), 0),
            ("testlab-kafkars-concurrent-multi-producer".to_owned(), 1),
        ])
    );
}

#[test]
fn observation_preserves_null_binary_and_ordered_headers() {
    let observed = normalize(
        7,
        CapturedRecord {
            topic: "records",
            partition: 2,
            offset: 11,
            key: Some(&[0, 255]),
            value: None,
            headers: vec![
                ("testlab-operation-id", Some(b"op-7")),
                ("testlab-sequence", Some(b"42")),
                ("nullable", None),
            ],
        },
    )
    .unwrap_or_else(|error| panic!("normalize observation: {error}"));

    assert_eq!(observed.operation_id.as_str(), "op-7");
    assert_eq!(observed.record.sequence, 42);
    assert_eq!(
        observed.record.key.as_ref().map(|key| key.encoding),
        Some(ByteEncoding::Hex)
    );
    assert_eq!(
        observed.record.key.as_ref().map(|key| key.data.as_str()),
        Some("00ff")
    );
    assert!(observed.record.value.is_none());
    assert!(observed.record.headers[2].value.is_none());
    assert_eq!(
        observed.digest,
        observed.record.digest().unwrap_or_default()
    );
}

#[test]
fn duplicate_operation_identity_is_invalid_observer_evidence() {
    let result = normalize(
        0,
        CapturedRecord {
            topic: "records",
            partition: 0,
            offset: 0,
            key: None,
            value: Some(b"value"),
            headers: vec![
                ("testlab-operation-id", Some(b"op-1")),
                ("testlab-operation-id", Some(b"op-2")),
                ("testlab-sequence", Some(b"1")),
            ],
        },
    );

    assert!(result.is_err());
}

#[test]
fn only_bounded_consumer_startup_errors_are_retried() {
    assert!(is_transient(&KafkaError::MessageConsumption(
        RDKafkaErrorCode::BrokerTransportFailure,
    )));
    assert!(!is_transient(&KafkaError::MessageConsumption(
        RDKafkaErrorCode::TopicAuthorizationFailed,
    )));
}

fn partition_expansion_scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-create-partitions.toml"
    ))
    .unwrap_or_else(|error| panic!("parse partition expansion scenario: {error}"))
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture identity: {error}"))
}
