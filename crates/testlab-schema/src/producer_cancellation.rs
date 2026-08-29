//! Producer cancellation keeps stage-aware public outcomes separate from delivery truth.

use serde::{Deserialize, Serialize};

use crate::{OperationId, ProducerId, RecordSpec};

/// Public stage-aware cancellation outcome.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProducerCancellationOutcome {
    /// Transport never owned the operation.
    CancelledNotSent,
    /// Transport may already own the operation.
    TooLate,
    /// The operation had already selected its terminal.
    AlreadyTerminal,
}

/// One bounded accepted send followed by two public cancellation requests.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CancelProducerSendCommand {
    /// Existing public producer.
    pub producer_id: ProducerId,
    /// Stable operation identity.
    pub operation_id: OperationId,
    /// Exact offered record.
    pub record: RecordSpec,
    /// Complete admission, cancellation, and terminal bound.
    pub timeout_ms: u64,
}

/// Ordered results from two cancellation requests on one retained observer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProducerCancellationCompletion {
    /// Stable operation identity.
    pub operation_id: OperationId,
    /// Exact first and second public cancellation outcomes.
    pub outcomes: Vec<ProducerCancellationOutcome>,
}
