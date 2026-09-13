//! Hosted consumer observations retain the configuration exposed by returned public handles.

use serde::{Deserialize, Serialize};

use crate::ConsumerId;

/// Exact values read from one successfully registered classic or KIP-848 consumer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupConsumerRegistrationObservation {
    /// Scenario-local handle identity carried by the registration command.
    pub consumer_id: ConsumerId,
    /// Public `Consumer::group_id` result.
    pub group_id: String,
    /// Public `Consumer::subscription` result in caller order.
    pub subscription: Vec<String>,
}

/// Exact values read from one successfully registered Share consumer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ShareConsumerRegistrationObservation {
    /// Scenario-local handle identity carried by the registration command.
    pub consumer_id: ConsumerId,
    /// Public `ShareConsumer::group_id` result.
    pub group_id: String,
    /// Public `ShareConsumer::subscription` result in caller order.
    pub subscription: Vec<String>,
    /// Public `ShareConsumer::rack` result.
    pub rack: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{GroupConsumerRegistrationObservation, ShareConsumerRegistrationObservation};
    use crate::{AdapterEvent, ConsumerId};

    #[test]
    fn registration_events_flatten_the_complete_public_observations() {
        let consumer_id =
            ConsumerId::new("consumer-1").unwrap_or_else(|error| panic!("consumer ID: {error}"));
        assert_round_trip(
            AdapterEvent::GroupConsumerCreated(GroupConsumerRegistrationObservation {
                consumer_id: consumer_id.clone(),
                group_id: "workers".to_owned(),
                subscription: vec!["orders".to_owned(), "returns".to_owned()],
            }),
            serde_json::json!({
                "kind": "group_consumer_created",
                "consumer_id": "consumer-1",
                "group_id": "workers",
                "subscription": ["orders", "returns"],
            }),
        );
        assert_round_trip(
            AdapterEvent::ShareConsumerCreated(ShareConsumerRegistrationObservation {
                consumer_id,
                group_id: "share-workers".to_owned(),
                subscription: vec!["orders".to_owned(), "returns".to_owned()],
                rack: Some("rack-a".to_owned()),
            }),
            serde_json::json!({
                "kind": "share_consumer_created",
                "consumer_id": "consumer-1",
                "group_id": "share-workers",
                "subscription": ["orders", "returns"],
                "rack": "rack-a",
            }),
        );
    }

    fn assert_round_trip(event: AdapterEvent, expected: serde_json::Value) {
        let value = serde_json::to_value(&event)
            .unwrap_or_else(|error| panic!("serialize registration event: {error}"));
        assert_eq!(value, expected);
        let decoded: AdapterEvent = serde_json::from_value(value)
            .unwrap_or_else(|error| panic!("deserialize registration event: {error}"));
        assert_eq!(decoded, event);
    }
}
