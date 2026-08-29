//! Transactional transforms preserve public input records, checkpoint coordinates, and output sets.

use serde::{Deserialize, Serialize};

use crate::{
    BatchRecord, ConsumedRecord, ConsumerId, GroupMembershipEpoch, OperationId, ProducerId,
};

/// Scenario intent for one consume-transform-produce transaction.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionalTransformAction {
    /// Existing public transactional producer.
    pub producer_id: ProducerId,
    /// Existing public group consumer.
    pub consumer_id: ConsumerId,
    /// Stable transaction identity.
    pub transaction_id: OperationId,
    /// Exact prior producer operation expected as the input record.
    pub expected_input_operation_id: OperationId,
    /// Ordered output records staged in the transaction.
    pub operations: Vec<BatchRecord>,
    /// Requested commit or abort outcome.
    pub disposition: crate::TransactionDisposition,
    /// Complete receive, send, offset-transfer, and end bound.
    pub timeout_ms: u64,
}

/// Adapter command omits the harness-only expected input identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionalTransformCommand {
    /// Existing public transactional producer.
    pub producer_id: ProducerId,
    /// Existing public group consumer.
    pub consumer_id: ConsumerId,
    /// Stable transaction identity.
    pub transaction_id: OperationId,
    /// Ordered output records staged in the transaction.
    pub operations: Vec<BatchRecord>,
    /// Requested commit or abort outcome.
    pub disposition: crate::TransactionDisposition,
    /// Complete public-operation bound.
    pub timeout_ms: u64,
}

/// Public completion of one transactional group-checkpoint transfer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionalTransformCompletion {
    /// Stable transaction identity.
    pub transaction_id: OperationId,
    /// Observed public transaction outcome.
    pub disposition: crate::TransactionDisposition,
    /// Group consumer that supplied the checkpoint.
    pub consumer_id: ConsumerId,
    /// Exact public input records.
    pub records: Vec<ConsumedRecord>,
    /// Public group identity retained by transactional metadata.
    pub group_id: String,
    /// Checkpoint topic.
    pub topic: String,
    /// Checkpoint partition.
    pub partition: i32,
    /// Checkpoint next offset transferred into the transaction.
    pub next_offset: i64,
    /// Public membership fence paired with the checkpoint.
    pub group_epoch: GroupMembershipEpoch,
}
