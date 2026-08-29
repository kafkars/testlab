//! Consumer-group offset mutation payloads retain exact partition identities.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// Scenario intent for one bounded committed-offset alteration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlterConsumerGroupOffsetAction {
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
    /// Exact nonnegative offset to commit.
    pub offset: i64,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded committed-offset alteration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlterConsumerGroupOffsetCommand {
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
    /// Exact nonnegative offset to commit.
    pub offset: i64,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Scenario intent for one bounded committed-offset deletion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteConsumerGroupOffsetAction {
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
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded committed-offset deletion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteConsumerGroupOffsetCommand {
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
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Public completion for one exact committed-offset mutation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConsumerGroupOffsetCompletion {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Exact Kafka topic name.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
}
