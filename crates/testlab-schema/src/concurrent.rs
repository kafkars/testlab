//! Concurrent actor contracts keep harness scheduling separate from public outcomes.

use serde::{Deserialize, Serialize};

use crate::{ActorId, ConcurrencyId, ConsumerId, OperationId, ProducerId, RecordSpec};

/// One public operation launched behind a harness-owned concurrent start boundary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConcurrentActor {
    /// Sends one record through a packaged public producer.
    ProducerSend {
        /// Stable actor identity.
        actor_id: ActorId,
        /// Existing public producer.
        producer_id: ProducerId,
        /// Stable producer operation identity.
        operation_id: OperationId,
        /// Exact logical record.
        record: RecordSpec,
    },
    /// Receives through one directly assigned packaged public consumer.
    AssignedReceive {
        /// Stable actor identity.
        actor_id: ActorId,
        /// Existing directly assigned consumer.
        consumer_id: ConsumerId,
        /// Stable receive operation identity.
        receive_id: OperationId,
        /// Producer operation that must be observed.
        expected_operation_id: OperationId,
        /// Complete public observation bound.
        timeout_ms: u64,
    },
}

impl ConcurrentActor {
    /// Returns the stable actor identity.
    pub fn actor_id(&self) -> &ActorId {
        match self {
            Self::ProducerSend { actor_id, .. } | Self::AssignedReceive { actor_id, .. } => {
                actor_id
            }
        }
    }

    /// Returns the stable public operation identity owned by this actor.
    pub fn operation_id(&self) -> &OperationId {
        match self {
            Self::ProducerSend { operation_id, .. } => operation_id,
            Self::AssignedReceive { receive_id, .. } => receive_id,
        }
    }
}

/// Starts one exact set of concurrent packaged-client actors.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StartConcurrentActorsAction {
    /// Stable concurrent group identity.
    pub concurrency_id: ConcurrencyId,
    /// Caller-ordered public actors released through one start barrier.
    pub actors: Vec<ConcurrentActor>,
}

/// Joins every actor from one prior start within one complete bound.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JoinConcurrentActorsAction {
    /// Stable concurrent group identity.
    pub concurrency_id: ConcurrencyId,
    /// Complete join bound.
    pub timeout_ms: u64,
}

/// One public actor command with scenario-only expectations removed.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConcurrentActorCommand {
    /// Sends one record through a packaged public producer.
    ProducerSend {
        /// Stable actor identity.
        actor_id: ActorId,
        /// Existing public producer.
        producer_id: ProducerId,
        /// Stable producer operation identity.
        operation_id: OperationId,
        /// Exact logical record.
        record: RecordSpec,
    },
    /// Receives through one directly assigned packaged public consumer.
    AssignedReceive {
        /// Stable actor identity.
        actor_id: ActorId,
        /// Existing directly assigned consumer.
        consumer_id: ConsumerId,
        /// Stable receive operation identity.
        receive_id: OperationId,
        /// Complete public observation bound.
        timeout_ms: u64,
    },
}

impl ConcurrentActorCommand {
    /// Returns the stable actor identity.
    pub fn actor_id(&self) -> &ActorId {
        match self {
            Self::ProducerSend { actor_id, .. } | Self::AssignedReceive { actor_id, .. } => {
                actor_id
            }
        }
    }

    /// Returns the stable public operation identity owned by this actor.
    pub fn operation_id(&self) -> &OperationId {
        match self {
            Self::ProducerSend { operation_id, .. } => operation_id,
            Self::AssignedReceive { receive_id, .. } => receive_id,
        }
    }
}

/// Protocol payload that releases one caller-ordered concurrent actor set.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StartConcurrentActorsCommand {
    /// Stable concurrent group identity.
    pub concurrency_id: ConcurrencyId,
    /// Caller-ordered public actor commands.
    pub actors: Vec<ConcurrentActorCommand>,
}
