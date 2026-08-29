//! Consumer-group admin payloads normalize discovery and lifecycle operations.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// Scenario intent for one bounded consumer-group listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListConsumerGroupsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Group identities that must appear in the public result.
    pub required_group_ids: Vec<String>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded consumer-group listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListConsumerGroupsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Scenario intent for one bounded consumer-group description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeConsumerGroupAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Exact public member count required by the scenario.
    pub expected_member_count: u32,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded consumer-group description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeConsumerGroupCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Scenario intent for one bounded consumer-group deletion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteConsumerGroupAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded consumer-group deletion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteConsumerGroupCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Public result for one consumer-group listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConsumerGroupsListing {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Sorted group identities reported by successful brokers.
    pub group_ids: Vec<String>,
    /// Sorted broker-scoped errors retained from the public result.
    pub broker_errors: Vec<AdminBrokerError>,
}

/// One broker-local error returned by a consumer-group listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminBrokerError {
    /// Broker that reported the error.
    pub broker_id: i32,
    /// Kafka protocol error code reported by that broker.
    pub code: i16,
}

/// Public result for one exact consumer-group description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConsumerGroupDescription {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Public member count reported by the adapter.
    pub member_count: u32,
}

/// Public completion for one exact consumer-group mutation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConsumerGroupCompletion {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact group reported as mutated.
    pub group_id: String,
}
