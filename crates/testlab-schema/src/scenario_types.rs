//! Scenario value types carry records, broker behavior, and deterministic expectations.

use serde::{Deserialize, Serialize};

use crate::{OperationId, RecordSpec, TerminalStatus};

/// One identified record within a public batch send.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BatchRecord {
    /// Stable operation identity.
    pub operation_id: OperationId,
    /// Exact logical record.
    pub record: RecordSpec,
}

/// One-shot behavior supported by the self-test model broker.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrokerBehavior {
    /// Persist once and acknowledge.
    Acknowledge,
    /// Persist once and close without a response.
    AcceptAndDropResponse,
    /// Reject before persistence.
    Reject,
    /// Persist twice and acknowledge, used by verifier tests.
    DuplicateAndAcknowledge,
    /// Persist corrupted bytes and acknowledge, used by verifier tests.
    CorruptAndAcknowledge,
}

/// Expected public and broker-visible result for one send.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OperationAssertion {
    /// Operation under test.
    pub operation_id: OperationId,
    /// Whether the public producer should accept ownership.
    pub accepted: bool,
    /// Expected terminal status for an accepted operation.
    pub terminal: Option<TerminalStatus>,
    /// Expected independent visibility.
    pub visibility: VisibilityExpectation,
}

/// Expected number of broker-visible records.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VisibilityExpectation {
    /// No matching record may exist.
    Absent,
    /// Exactly one matching record must exist.
    ExactlyOnce,
    /// Zero or one matching record may exist.
    ZeroOrOne,
}
