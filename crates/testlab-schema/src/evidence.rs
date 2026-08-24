//! Ordered run evidence keeps adapter claims separate from broker observations.

use serde::{Deserialize, Serialize};

use crate::{
    AdapterDescriptor, AdapterEventEnvelope, BrokerBehavior, CommandEnvelope, EnvironmentId,
    EnvironmentOperationId, OperationId, RecordSpec, RunId, ScenarioId, SubjectId, VerdictStatus,
};

/// Current sealed evidence manifest version.
pub const EVIDENCE_SCHEMA_VERSION: u16 = 5;

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
    /// One completed environment terminal operation.
    EnvironmentOperation {
        /// Exact correlated environment operation.
        operation: EnvironmentOperation,
    },
    /// Harness or environment failure.
    HarnessError {
        /// Stable bounded failure.
        error: HarnessError,
    },
}

/// One effectful environment operation and its terminal outcome.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentOperation {
    /// Stable operation identity.
    pub id: EnvironmentOperationId,
    /// Semantic operation class.
    pub kind: EnvironmentOperationKind,
    /// Executed program name.
    pub program: String,
    /// Exact non-secret arguments.
    pub args: Vec<String>,
    /// Diagnostic operation start time.
    pub started_unix_ms: u64,
    /// Diagnostic operation completion time.
    pub completed_unix_ms: u64,
    /// Terminal operation status.
    pub status: EnvironmentOperationStatus,
    /// Process exit code when available.
    pub exit_code: Option<i32>,
    /// Sealed stdout artifact name when retained.
    pub stdout_artifact: Option<String>,
    /// Sealed stderr artifact name when retained.
    pub stderr_artifact: Option<String>,
    /// Bounded failure diagnostic when unsuccessful.
    pub diagnostic: Option<String>,
}

/// Environment operation classes retained in evidence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentOperationKind {
    /// Pull one exact content-addressed broker image.
    ImagePull,
    /// Inspect one immutable broker image.
    ImageInspect,
    /// Resolve and validate Compose configuration.
    ComposeConfig,
    /// Start one isolated Compose project.
    ComposeUp,
    /// Probe broker API readiness.
    Readiness,
    /// Provision environment-owned broker authentication state.
    BrokerSecuritySetup,
    /// Establish one explicit broker feature level after readiness.
    BrokerFeatureSetup,
    /// Provision scenario-owned broker resources.
    BrokerProvision,
    /// Restart one declared broker service and retain its terminal result.
    BrokerRestart,
    /// Independently observe one partition leader before or after disruption.
    BrokerLeaderObserve,
    /// Stop one independently selected partition leader.
    BrokerStop,
    /// Start one previously stopped partition leader.
    BrokerStart,
    /// Snapshot broker-visible records with an independent client.
    BrokerObserve,
    /// Capture Compose process state.
    ComposePs,
    /// Capture broker logs.
    ComposeLogs,
    /// Stop the project and remove owned volumes.
    ComposeDown,
}

/// Terminal status for one environment operation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentOperationStatus {
    /// The process completed successfully.
    Succeeded,
    /// The process completed unsuccessfully.
    Failed,
    /// The harness killed the process at its deadline.
    TimedOut,
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
    /// Independently controlled environment used by the run.
    pub environment_id: EnvironmentId,
    /// Start time in Unix milliseconds.
    pub started_unix_ms: u64,
    /// Completion time in Unix milliseconds.
    pub completed_unix_ms: u64,
    /// Adapter identity when the handshake succeeded.
    pub adapter: Option<AdapterDescriptor>,
    /// Deterministic final status.
    pub status: VerdictStatus,
}
