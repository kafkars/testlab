//! Metadata-quorum payloads preserve stable Raft replica and endpoint state.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// Scenario intent for one bounded metadata-quorum description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeMetadataQuorumAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded metadata-quorum description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeMetadataQuorumCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One voter or observer in canonical replica-ID order.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataQuorumReplicaState {
    /// Exact nonnegative replica identity.
    pub replica_id: i32,
    /// Kafka's unpadded URL-safe base64 directory identity, when represented.
    pub replica_directory_id: Option<String>,
    /// Log-end offset, or absence for Kafka's unknown sentinel.
    pub log_end_offset: Option<i64>,
    /// Last-fetch timestamp, or absence when unknown or unrepresented.
    pub last_fetch_timestamp_ms: Option<i64>,
    /// Last-caught-up timestamp, or absence when unknown or unrepresented.
    pub last_caught_up_timestamp_ms: Option<i64>,
}

/// One named controller listener endpoint.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataQuorumListenerState {
    /// Exact listener name.
    pub name: String,
    /// Exact advertised hostname.
    pub host: String,
    /// Exact nonzero advertised port.
    pub port: u16,
}

/// One quorum node and its canonically ordered listener endpoints.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataQuorumNodeState {
    /// Exact nonnegative node identity.
    pub node_id: i32,
    /// Listener endpoints in canonical name order.
    pub listeners: Vec<MetadataQuorumListenerState>,
}

/// Public metadata-quorum result exposed by the packaged client.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminMetadataQuorumDescription {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Current leader identity, or absence for Kafka's unknown sentinel.
    pub leader_id: Option<i32>,
    /// Exact nonnegative leader epoch.
    pub leader_epoch: i32,
    /// Exact nonnegative high watermark.
    pub high_watermark: i64,
    /// Current voters in canonical replica-ID order.
    pub voters: Vec<MetadataQuorumReplicaState>,
    /// Current observers in canonical replica-ID order.
    pub observers: Vec<MetadataQuorumReplicaState>,
    /// Version-two node endpoints, or absence when unrepresented.
    pub nodes: Option<Vec<MetadataQuorumNodeState>>,
}

/// Independent metadata-quorum snapshot from Kafka's pinned administration CLI.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerMetadataQuorumState {
    /// Monotonic observation ordinal from the environment.
    pub observation: u64,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Current leader identity, or absence for Kafka's unknown sentinel.
    pub leader_id: Option<i32>,
    /// Exact nonnegative leader epoch.
    pub leader_epoch: i32,
    /// Exact nonnegative high watermark.
    pub high_watermark: i64,
    /// Current voters joined across CLI status and replication views.
    pub voters: Vec<MetadataQuorumReplicaState>,
    /// Current observers joined across CLI status and replication views.
    pub observers: Vec<MetadataQuorumReplicaState>,
    /// Controller endpoints visible in the CLI status view.
    pub nodes: Vec<MetadataQuorumNodeState>,
}

#[cfg(test)]
#[path = "admin_metadata_quorum_test.rs"]
mod tests;
