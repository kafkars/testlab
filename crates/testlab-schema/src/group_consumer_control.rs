//! Group-consumer controls keep public runtime mutations operation-identified.

use serde::{Deserialize, Serialize};

use crate::{AssignedStartPosition, ConsumerId, OperationId, TopicPartitionIdentity};

/// One public runtime mutation for a hosted group consumer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GroupConsumerControl {
    /// Suspends Fetch progress for the complete partition set.
    Pause {
        /// Ordered unique current assignment partitions.
        partitions: Vec<TopicPartitionIdentity>,
    },
    /// Resumes Fetch progress for the complete partition set.
    Resume {
        /// Ordered unique current assignment partitions.
        partitions: Vec<TopicPartitionIdentity>,
    },
    /// Replaces one assignment-fenced next-fetch position.
    Seek {
        /// Current assignment partition.
        partition: TopicPartitionIdentity,
        /// Explicit replacement position.
        position: AssignedStartPosition,
    },
}

impl GroupConsumerControl {
    /// Returns the structural public operation kind.
    pub const fn kind(&self) -> GroupConsumerControlKind {
        match self {
            Self::Pause { .. } => GroupConsumerControlKind::Pause,
            Self::Resume { .. } => GroupConsumerControlKind::Resume,
            Self::Seek { .. } => GroupConsumerControlKind::Seek,
        }
    }
}

/// Stable structural identity of one completed group control.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupConsumerControlKind {
    /// Fetch suspension.
    Pause,
    /// Fetch resumption.
    Resume,
    /// Position replacement.
    Seek,
}

/// Scenario request for one bounded group-consumer control.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupConsumerControlAction {
    /// Stable public operation identity.
    pub operation_id: OperationId,
    /// Existing hosted group consumer.
    pub consumer_id: ConsumerId,
    /// Exact runtime mutation.
    pub control: GroupConsumerControl,
    /// Complete admission or observation bound.
    pub timeout_ms: u64,
}

/// Expectation-free wire request for one group-consumer control.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupConsumerControlCommand {
    /// Stable public operation identity.
    pub operation_id: OperationId,
    /// Existing hosted group consumer.
    pub consumer_id: ConsumerId,
    /// Exact runtime mutation.
    pub control: GroupConsumerControl,
    /// Complete admission or observation bound.
    pub timeout_ms: u64,
}

/// Public successful completion of one group-consumer control.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupConsumerControlCompletion {
    /// Stable public operation identity.
    pub operation_id: OperationId,
    /// Controlled hosted consumer.
    pub consumer_id: ConsumerId,
    /// Exact completed structural operation.
    pub control: GroupConsumerControlKind,
}
