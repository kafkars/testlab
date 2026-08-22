//! Versioned data contracts shared across the testlab trust boundary.

mod adapter;
mod bytes;
mod contract;
mod environment;
mod evidence;
mod ids;
mod pack;
mod protocol;
mod qualification;
mod qualification_evidence;
mod record;
mod scenario;
mod scenario_action_validation;
mod scenario_validation;
mod subject;
mod verdict;

pub use adapter::{AdapterDescriptor, Capability};
pub use bytes::{ByteEncoding, ByteString, ByteStringError};
pub use contract::{ContractDefinition, ContractRegistry, ContractRegistryError};
pub use environment::{
    Authentication, BrokerIdentity, ENVIRONMENT_SCHEMA_VERSION, EnvironmentDriver,
    EnvironmentError, EnvironmentManifest, SecurityProfile, TransportSecurity,
};
pub use evidence::{
    BrokerObservation, EVIDENCE_SCHEMA_VERSION, EnvironmentOperation, EnvironmentOperationKind,
    EnvironmentOperationStatus, EvidenceManifest, HarnessError, HistoryEntry, HistoryPayload,
};
pub use ids::{
    AdapterId, CellId, ClientId, CommandId, ContractId, EnvironmentId, EnvironmentOperationId,
    IdError, OperationId, PackId, ProducerId, QualificationId, RunId, ScenarioId, StepId,
    SubjectId,
};
pub use pack::{ScenarioPack, ScenarioPackError};
pub use protocol::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, AdapterSaslMechanism, AdapterSecurity,
    CommandEnvelope, PROTOCOL_VERSION, SASL_PASSWORD_ENVIRONMENT, SASL_USERNAME_ENVIRONMENT,
    TLS_CA_PEM_ENVIRONMENT, TerminalStatus,
};
pub use qualification::{
    QUALIFICATION_SCHEMA_VERSION, QualificationCell, QualificationError, QualificationManifest,
};
pub use qualification_evidence::{
    QUALIFICATION_EVIDENCE_SCHEMA_VERSION, QualificationCellEvidence, QualificationEvidenceError,
    QualificationEvidenceManifest, QualificationRunEvidence,
};
pub use record::{HeaderSpec, RecordError, RecordSpec};
pub use scenario::{
    BrokerBehavior, OperationAssertion, SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction,
    ScenarioError, ScenarioStep, VisibilityExpectation,
};
pub use subject::{SUBJECT_SCHEMA_VERSION, SubjectArtifact, SubjectError, SubjectManifest};
pub use verdict::{Verdict, VerdictStatus, Violation};

#[cfg(test)]
mod bytes_test;
#[cfg(test)]
mod environment_test;
#[cfg(test)]
mod ids_test;
#[cfg(test)]
mod qualification_evidence_test;
#[cfg(test)]
mod qualification_test;
#[cfg(test)]
mod record_test;
#[cfg(test)]
mod scenario_test;
#[cfg(test)]
mod subject_test;
