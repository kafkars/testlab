//! Consumer-group admin payloads normalize discovery and lifecycle operations.

#[path = "admin_consumer_group_description_batch.rs"]
pub(crate) mod description_batch;

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// Public Kafka group-listing operation selected by a scenario.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupListingApi {
    /// Uses the consumer-only compatibility view.
    #[default]
    ConsumerGroups,
    /// Uses the generic `ListGroups` view without narrowing group types.
    AllGroups,
}

/// Scenario intent for one bounded consumer-group listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListConsumerGroupsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Public group-listing operation exercised by the adapter.
    #[serde(default)]
    pub api: GroupListingApi,
    /// Caller-ordered broker-side group-state filters.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub state_filters: Vec<String>,
    /// Caller-ordered broker-side group-type filters.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_type_filters: Vec<String>,
    /// Caller-ordered client-side protocol-type filters for the generic API.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub protocol_type_filters: Vec<String>,
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
    /// Public group-listing operation exercised by the adapter.
    #[serde(default)]
    pub api: GroupListingApi,
    /// Caller-ordered broker-side group-state filters.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub state_filters: Vec<String>,
    /// Caller-ordered broker-side group-type filters.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_type_filters: Vec<String>,
    /// Caller-ordered client-side protocol-type filters for the generic API.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub protocol_type_filters: Vec<String>,
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
    /// Whether Kafka must return the authorized-operation bitfield.
    #[serde(default)]
    pub include_authorized_operations: bool,
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
    /// Whether Kafka must return the authorized-operation bitfield.
    pub include_authorized_operations: bool,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Scenario intent for one bounded Share-group description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeShareGroupAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka Share-group identity.
    pub group_id: String,
    /// Exact public and independently observed group state.
    pub expected_state: String,
    /// Exact public and independently observed member count.
    pub expected_member_count: u32,
    /// Exact public member rack identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_rack_id: Option<String>,
    /// Topic that must appear in the public member subscription and assignment.
    pub expected_topic: String,
    /// Partition that must appear in the public member assignment.
    pub expected_partition: i32,
    /// Whether Kafka must return the authorized-operation bitfield.
    pub include_authorized_operations: bool,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded Share-group description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeShareGroupCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka Share-group identity.
    pub group_id: String,
    /// Whether Kafka must return the authorized-operation bitfield.
    pub include_authorized_operations: bool,
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
    /// Raw Kafka authorization bitfield, when requested.
    pub authorized_operations: Option<i32>,
}

/// One public Share-group member and its exact current assignment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminShareGroupMember {
    /// Stable broker-issued member identity.
    pub member_id: String,
    /// Rack identity returned by Kafka.
    pub rack_id: Option<String>,
    /// Exact signed member epoch.
    pub member_epoch: i32,
    /// Public client identity reported by Kafka.
    pub client_id: String,
    /// Sorted subscribed topic names.
    pub subscribed_topics: Vec<String>,
    /// Deterministically ordered topic assignments.
    pub assignments: Vec<AdminShareGroupTopicAssignment>,
}

/// One topic and its exact partition assignment for a Share-group member.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminShareGroupTopicAssignment {
    /// Broker-issued nonzero topic identity.
    pub topic_id: [u8; 16],
    /// Correlated UTF-8 topic name.
    pub topic: String,
    /// Sorted nonnegative assigned partitions.
    pub partitions: Vec<i32>,
}

/// Public result for one exact Share-group description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminShareGroupDescription {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact Kafka Share-group identity.
    pub group_id: String,
    /// Broker-reported group state.
    pub state: String,
    /// Exact signed group epoch.
    pub group_epoch: i32,
    /// Exact signed target-assignment epoch.
    pub assignment_epoch: i32,
    /// Broker-selected server assignor.
    pub assignor_name: String,
    /// Raw Kafka authorization bitfield, when requested.
    pub authorized_operations: Option<i32>,
    /// Members ordered by broker-issued member identity.
    pub members: Vec<AdminShareGroupMember>,
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
