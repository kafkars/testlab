//! Scenario value types carry records, broker behavior, and deterministic expectations.

use serde::{Deserialize, Serialize};

use crate::{
    ClientId, OperationId, ProducerId, RecordSpec, ScenarioAction, StepId, TerminalStatus,
};

/// One named scenario action.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScenarioStep {
    /// Stable step identity.
    pub id: StepId,
    /// Action payload.
    #[serde(flatten)]
    pub action: ScenarioAction,
}

/// Creates one client with an optional exact broker-cluster identity guard.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateClientAction {
    /// Scenario-local client identity.
    pub client_id: ClientId,
    /// Exact broker-issued cluster ID required during construction and readiness.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_cluster_id: Option<String>,
    /// Expected normalized public failure; retained only in scenario intent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_error_code: Option<String>,
}

/// Creates one public client with an optional exact broker-cluster identity guard.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateClientCommand {
    /// Scenario-local client identity.
    pub client_id: ClientId,
    /// Exact broker-issued cluster ID required by the public client.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_cluster_id: Option<String>,
}

/// One identified record within a public batch send.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BatchRecord {
    /// Stable operation identity.
    pub operation_id: OperationId,
    /// Exact logical record.
    pub record: RecordSpec,
}

/// Request to close one idle transactional producer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CloseTransactionalProducerAction {
    /// Transactional producer to close.
    pub producer_id: ProducerId,
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

/// Requested terminal operation for one public transaction.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionDisposition {
    /// Makes every staged record visible to read-committed observers.
    Commit,
    /// Keeps every staged record invisible to read-committed observers.
    Abort,
    /// Aborts one staged partition through the public Admin API.
    AdminPartitionAbort,
}

/// Public operation used to fence one active transactional producer.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionFenceMethod {
    /// Initializes a replacement producer with the same transactional ID.
    #[default]
    ReplacementInitialization,
    /// Uses the Admin singleton transaction-termination operation.
    AdminForceTermination,
}

/// Expected public and broker-visible result for one send.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OperationAssertion {
    /// Operation under test.
    pub operation_id: OperationId,
    /// Whether the public producer should accept ownership.
    pub accepted: bool,
    /// Expected terminal status, omitted only for stage-aware cancellation.
    pub terminal: Option<TerminalStatus>,
    /// Expected independent visibility.
    pub visibility: VisibilityExpectation,
    /// Exact normalized public operation failure when one is required.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_error_code: Option<String>,
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
