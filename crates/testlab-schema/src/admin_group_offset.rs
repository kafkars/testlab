//! Consumer-group offset payloads keep verifier expectations out of adapter commands.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// Scenario intent for one bounded consumer-group offset listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListConsumerGroupOffsetsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Whether the public request requires a stable group state.
    pub require_stable: bool,
    /// Exact nonnegative committed offset the verifier requires.
    pub expected_offset: i64,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded consumer-group offset listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListConsumerGroupOffsetsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Whether the public request requires a stable group state.
    pub require_stable: bool,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Public result for one exact committed-offset listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConsumerGroupOffsetListing {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Reported committed offset, or no offset when Kafka has no value.
    pub offset: Option<i64>,
}
