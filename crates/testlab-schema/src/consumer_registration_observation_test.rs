//! Tests for the consumer registration observation contract.

use super::{
    GroupConsumerRegistrationObservation, ShareConsumerBuilderSelection,
    ShareConsumerRegistrationObservation,
};
use crate::{
    AdapterEvent, ConsumerId, GroupConsumerConfigurationSelection, GroupOffsetReset, GroupProtocol,
    GroupReadIsolation,
};

#[test]
fn registration_events_flatten_the_complete_public_observations() {
    let consumer_id =
        ConsumerId::new("consumer-1").unwrap_or_else(|error| panic!("consumer ID: {error}"));
    assert_round_trip(
        AdapterEvent::GroupConsumerCreated(GroupConsumerRegistrationObservation {
            consumer_id: consumer_id.clone(),
            group_id: "workers".to_owned(),
            subscription: vec!["orders".to_owned(), "returns".to_owned()],
            selected_protocol: GroupProtocol::Classic,
            selected_configuration: Some(GroupConsumerConfigurationSelection {
                offset_reset: GroupOffsetReset::Earliest,
                read_isolation: GroupReadIsolation::ReadUncommitted,
                fetch: None,
                limits: None,
                processing_timeout_ms: Some(61_000),
                membership_start_timeout_ms: None,
                seek_timeout_ms: None,
                close_timeout_ms: None,
                group_instance_id: None,
                classic_assignor: None,
                classic_session_timeout_ms: None,
                classic_rebalance_timeout_ms: None,
                classic_heartbeat_interval_ms: None,
                classic_heartbeat_attempt_timeout_ms: None,
                classic_rejoin_backoff_ms: None,
                classic_rejoin_attempt_timeout_ms: None,
            }),
        }),
        serde_json::json!({
            "kind": "group_consumer_created",
            "consumer_id": "consumer-1",
            "group_id": "workers",
            "subscription": ["orders", "returns"],
            "selected_protocol": "classic",
            "selected_configuration": {
                "offset_reset": "earliest",
                "read_isolation": "read_uncommitted",
                "processing_timeout_ms": 61000,
            },
        }),
    );
    assert_round_trip(
        AdapterEvent::ShareConsumerCreated(ShareConsumerRegistrationObservation {
            consumer_id,
            group_id: "share-workers".to_owned(),
            subscription: vec!["orders".to_owned(), "returns".to_owned()],
            rack: Some("rack-a".to_owned()),
            selected_builder: ShareConsumerBuilderSelection {
                rack: Some("rack-a".to_owned()),
                fetch: None,
                membership_start_timeout_ns: 29_500_000_000,
                close_timeout_ms: 10_000,
            },
        }),
        serde_json::json!({
            "kind": "share_consumer_created",
            "consumer_id": "consumer-1",
            "group_id": "share-workers",
            "subscription": ["orders", "returns"],
            "rack": "rack-a",
            "selected_builder": {
                "rack": "rack-a",
                "membership_start_timeout_ns": 29_500_000_000_u64,
                "close_timeout_ms": 10000,
            },
        }),
    );
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the fixture helper owns both constructed comparison values"
)]
fn assert_round_trip(event: AdapterEvent, expected: serde_json::Value) {
    let value = serde_json::to_value(&event)
        .unwrap_or_else(|error| panic!("serialize registration event: {error}"));
    assert_eq!(value, expected);
    let decoded: AdapterEvent = serde_json::from_value(value)
        .unwrap_or_else(|error| panic!("deserialize registration event: {error}"));
    assert_eq!(decoded, event);
}
