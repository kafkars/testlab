//! Streams-group lifecycle payloads retain public results and final CLI absence.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

#[path = "admin_streams_group_validation.rs"]
pub(crate) mod validation;

#[cfg(test)]
#[path = "admin_streams_group_test.rs"]
mod tests;

/// Input topic fixed by Apache Kafka's bundled `WordCount` Streams example.
pub const STREAMS_DEMO_INPUT_TOPIC: &str = "streams-plaintext-input";
/// Output topic fixed by Apache Kafka's bundled `WordCount` Streams example.
pub const STREAMS_DEMO_OUTPUT_TOPIC: &str = "streams-wordcount-output";

/// One bounded exercise of the public Streams-group Admin family.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExerciseStreamsGroupAdminLifecycleAction {
    /// Existing client whose Admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the composite public operation.
    pub operation_id: OperationId,
    /// Primary Streams application identity mutated by the lifecycle.
    pub primary_group_id: String,
    /// Second Streams application identity used by plural operations.
    pub secondary_group_id: String,
    /// Exact input topic owned by the real Streams fixture.
    pub input_topic: String,
    /// Exact output topic owned by the real Streams fixture.
    pub output_topic: String,
    /// Committed input position established independently for both groups.
    pub expected_initial_offset: i64,
    /// Different valid input position written through the public Admin API.
    pub altered_offset: i64,
    /// Whether descriptions request Kafka authorization metadata.
    #[serde(default = "default_streams_option")]
    pub include_authorized_operations: bool,
    /// Whether descriptions request Kafka's full topology graph.
    #[serde(default = "default_streams_option")]
    pub include_topology_description: bool,
    /// Whether offset listings require stable committed positions.
    #[serde(default = "default_streams_option")]
    pub require_stable: bool,
    /// Complete nine-operation public bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded Streams-group Admin lifecycle.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExerciseStreamsGroupAdminLifecycleCommand {
    /// Existing client whose Admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the composite public operation.
    pub operation_id: OperationId,
    /// Primary Streams application identity mutated by the lifecycle.
    pub primary_group_id: String,
    /// Second Streams application identity used by plural operations.
    pub secondary_group_id: String,
    /// Exact input topic selected by the public offset operations.
    pub input_topic: String,
    /// Different valid input position written through the public Admin API.
    pub altered_offset: i64,
    /// Whether descriptions request Kafka authorization metadata.
    pub include_authorized_operations: bool,
    /// Whether descriptions request Kafka's full topology graph.
    pub include_topology_description: bool,
    /// Whether offset listings require stable committed positions.
    pub require_stable: bool,
    /// Complete nine-operation public bound.
    pub timeout_ms: u64,
}

const fn default_streams_option() -> bool {
    true
}

/// Bounded public facts from one successful Streams-group description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminStreamsGroupDescription {
    /// Exact requested application identity.
    pub group_id: String,
    /// Stable broker group-state string.
    pub state: String,
    /// Exact signed group epoch.
    pub group_epoch: i32,
    /// Exact signed assignment epoch.
    pub assignment_epoch: i32,
    /// Initialized topology epoch, when present.
    pub topology_epoch: Option<i32>,
    /// Sorted source topics from initialized subtopologies.
    pub topology_source_topics: Vec<String>,
    /// Initialized subtopology count, preserving nullable broker state.
    pub topology_subtopology_count: Option<u32>,
    /// Current member count.
    pub member_count: u32,
    /// Raw authorization bitfield requested from Kafka.
    pub authorized_operations: Option<i32>,
    /// Raw v1 full-topology availability status.
    pub topology_description_status: Option<i8>,
    /// Full-description subtopology count when Kafka returned the graph.
    pub topology_description_subtopology_count: Option<u32>,
}

/// One successful public committed-offset result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminStreamsGroupOffset {
    /// Exact Streams application identity.
    pub group_id: String,
    /// Exact input topic.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
    /// Committed next offset, or none after deletion.
    pub committed_offset: Option<i64>,
    /// Optional committed leader epoch.
    pub leader_epoch: Option<i32>,
    /// Optional committed Kafka metadata.
    pub metadata: Option<String>,
}

/// Exact target accepted by a successful public offset mutation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminStreamsGroupPartition {
    /// Exact Streams application identity.
    pub group_id: String,
    /// Exact input topic.
    pub topic: String,
    /// Exact nonnegative partition.
    pub partition: i32,
}

/// Public results from all seven Streams-group Admin methods in one lifecycle.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminStreamsGroupAdminLifecycle {
    /// Stable identity for the composite public operation.
    pub operation_id: OperationId,
    /// Singular description result.
    pub singular_description: AdminStreamsGroupDescription,
    /// Plural descriptions in exact caller order.
    pub plural_descriptions: Vec<AdminStreamsGroupDescription>,
    /// Singular initial selected-offset result.
    pub singular_initial_offset: AdminStreamsGroupOffset,
    /// Plural initial selected-offset results in exact caller order.
    pub plural_initial_offsets: Vec<AdminStreamsGroupOffset>,
    /// Singular selected-offset result after public alteration.
    pub offset_after_alter: AdminStreamsGroupOffset,
    /// Exact offset target returned successfully by public deletion.
    pub deleted_offset: AdminStreamsGroupPartition,
    /// Singular selected-offset result after public deletion.
    pub offset_after_delete: AdminStreamsGroupOffset,
    /// Successfully deleted groups in exact caller order.
    pub deleted_group_ids: Vec<String>,
    /// Throttles for describe-one, describe-many, list-one, list-many, alter,
    /// list-after-alter, delete-offset, list-after-delete, and delete-groups.
    pub throttle_times_ms: [u64; 9],
}

/// Independent final Streams-group absence from Kafka's pinned CLI.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerStreamsGroupsState {
    /// Monotonic observation ordinal from the environment.
    pub observation: u64,
    /// Stable identity for the composite public operation.
    pub operation_id: OperationId,
    /// Group identities checked in public deletion order.
    pub group_ids: Vec<String>,
    /// Whether every selected group was absent from the CLI result.
    pub all_absent: bool,
}
