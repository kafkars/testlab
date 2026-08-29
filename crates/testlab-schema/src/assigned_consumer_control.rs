//! Direct-consumer controls separate stable command identity from later record truth.

use serde::{Deserialize, Serialize};

use crate::{ConsumerId, OperationId, TopicPartitionIdentity};

/// One explicit public direct-consumer start position.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AssignedStartPosition {
    /// Resolve the earliest available offset.
    Beginning,
    /// Resolve the current log end.
    End,
    /// Use one exact nonnegative next-fetch offset.
    Offset {
        /// Exact next-fetch offset.
        offset: i64,
    },
}

/// One direct assignment entry with an explicit initial position.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssignedPartitionPosition {
    /// Exact topic spelling.
    pub topic: String,
    /// Zero-based partition index.
    pub partition: i32,
    /// Explicit initial position.
    pub position: AssignedStartPosition,
}

/// Public mutation requested against one directly assigned consumer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AssignedConsumerControl {
    /// Atomically replaces the complete assignment.
    Replace {
        /// Ordered unique replacement entries.
        partitions: Vec<AssignedPartitionPosition>,
    },
    /// Atomically adds entries without disturbing surviving cursors.
    Add {
        /// Ordered unique additions.
        partitions: Vec<AssignedPartitionPosition>,
    },
    /// Atomically removes entries without disturbing surviving cursors.
    Remove {
        /// Ordered unique removals.
        partitions: Vec<TopicPartitionIdentity>,
    },
    /// Replaces one assigned partition's next-fetch position.
    Seek {
        /// Existing assigned partition.
        partition: TopicPartitionIdentity,
        /// Explicit replacement position.
        position: AssignedStartPosition,
    },
    /// Suspends delivery for one assigned partition.
    Pause {
        /// Existing assigned partition.
        partition: TopicPartitionIdentity,
    },
    /// Resumes delivery for one paused assigned partition.
    Resume {
        /// Existing assigned partition.
        partition: TopicPartitionIdentity,
    },
}

impl AssignedConsumerControl {
    /// Returns the structural control kind without duplicating its input.
    pub const fn kind(&self) -> AssignedConsumerControlKind {
        match self {
            Self::Replace { .. } => AssignedConsumerControlKind::Replace,
            Self::Add { .. } => AssignedConsumerControlKind::Add,
            Self::Remove { .. } => AssignedConsumerControlKind::Remove,
            Self::Seek { .. } => AssignedConsumerControlKind::Seek,
            Self::Pause { .. } => AssignedConsumerControlKind::Pause,
            Self::Resume { .. } => AssignedConsumerControlKind::Resume,
        }
    }
}

/// Stable structural identity for one completed control.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignedConsumerControlKind {
    /// Complete replacement.
    Replace,
    /// Incremental addition.
    Add,
    /// Incremental removal.
    Remove,
    /// Position replacement.
    Seek,
    /// Delivery suspension.
    Pause,
    /// Delivery resumption.
    Resume,
}

/// Scenario request for one bounded direct-consumer control.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssignedConsumerControlAction {
    /// Stable control identity.
    pub operation_id: OperationId,
    /// Existing assigned consumer.
    pub consumer_id: ConsumerId,
    /// Exact public mutation.
    pub control: AssignedConsumerControl,
    /// Complete admission and position-resolution bound.
    pub timeout_ms: u64,
}

/// Expectation-free wire request for one bounded direct-consumer control.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssignedConsumerControlCommand {
    /// Stable control identity.
    pub operation_id: OperationId,
    /// Existing assigned consumer.
    pub consumer_id: ConsumerId,
    /// Exact public mutation.
    pub control: AssignedConsumerControl,
    /// Complete admission and position-resolution bound.
    pub timeout_ms: u64,
}

/// Public successful completion of one direct-consumer control.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssignedConsumerControlCompletion {
    /// Stable control identity.
    pub operation_id: OperationId,
    /// Controlled consumer.
    pub consumer_id: ConsumerId,
    /// Exact completed structural operation.
    pub control: AssignedConsumerControlKind,
}
