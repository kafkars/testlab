//! Ordered run evidence keeps adapter claims separate from broker observations.

use serde::{Deserialize, Serialize};

use crate::{
    AdapterDescriptor, AdapterEventEnvelope, BrokerBehavior, CommandEnvelope, OperationId,
    RecordSpec, RunId, ScenarioId, SubjectId, VerdictStatus,
};

/// One record independently observed by the broker environment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerObservation {
    /// Monotonic observation ordinal from the environment.
    pub observation: u64,
    /// Broker-assigned logical offset in the self-test environment.
    pub offset: i64,
    /// Operation identity carried by the adapter request.
    pub operation_id: OperationId,
    /// Record bytes actually received by the environment.
    pub record: RecordSpec,
    /// Canonical digest calculated from the received record.
    pub digest: String,
}

/// One ordered entry in the complete run history.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HistoryEntry {
    /// Global history order assigned by testctl.
    pub sequence: u64,
    /// Diagnostic wall-clock observation time.
    pub observed_unix_ms: u64,
    /// Typed history payload.
    #[serde(flatten)]
    pub payload: HistoryPayload,
}

/// Event sources retained in the run history.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum HistoryPayload {
    /// Command sent to the adapter.
    HarnessCommand {
        /// Exact command envelope.
        command: CommandEnvelope,
    },
    /// Event received from the adapter.
    AdapterEvent {
        /// Exact event envelope.
        event: AdapterEventEnvelope,
    },
    /// One-shot model-broker behavior selected by testctl.
    BrokerControl {
        /// Selected behavior.
        behavior: BrokerBehavior,
    },
    /// External record observation.
    BrokerObservation {
        /// Exact observation.
        observation: BrokerObservation,
    },
    /// Harness or environment failure.
    HarnessError {
        /// Stable bounded failure.
        error: HarnessError,
    },
}

/// Stable invalidity evidence from testctl or an environment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessError {
    /// Stable machine-readable code.
    pub code: String,
    /// Bounded human-readable diagnostic.
    pub diagnostic: String,
}

/// Top-level sealed run identity and status.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceManifest {
    /// Evidence schema version.
    pub schema_version: u16,
    /// Unique attempt identity.
    pub run_id: RunId,
    /// Scenario under test.
    pub scenario_id: ScenarioId,
    /// Packaged subject under test.
    pub subject_id: SubjectId,
    /// Start time in Unix milliseconds.
    pub started_unix_ms: u64,
    /// Completion time in Unix milliseconds.
    pub completed_unix_ms: u64,
    /// Adapter identity when the handshake succeeded.
    pub adapter: Option<AdapterDescriptor>,
    /// Deterministic final status.
    pub status: VerdictStatus,
}
