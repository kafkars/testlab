//! Versioned data contracts shared across the testlab trust boundary.

mod adapter;
mod admin_action_validation;
mod bytes;
mod consumer_action_validation;
mod contract;
mod environment;
mod evidence;
mod ids;
mod pack;
mod protocol;
mod protocol_command;
mod protocol_event;
mod protocol_group;
mod protocol_security;
mod qualification;
mod qualification_evidence;
mod receive_action_validation;
mod record;
mod scenario;
mod scenario_action;
mod scenario_action_validation;
mod scenario_capability_validation;
mod scenario_environment_action_validation;
mod scenario_error;
mod scenario_types;
mod scenario_validation;
mod share;
mod share_action_validation;
mod subject;
mod transaction_action_validation;
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
    AdapterId, CellId, ClientId, CommandId, ConsumerId, ContractId, EnvironmentId,
    EnvironmentOperationId, IdError, OperationId, PackId, ProducerId, QualificationId, RunId,
    ScenarioId, StepId, SubjectId,
};
pub use pack::{ScenarioPack, ScenarioPackError};
pub use protocol::{AdapterEventEnvelope, CommandEnvelope, PROTOCOL_VERSION};
pub use protocol_command::AdapterCommand;
pub use protocol_event::AdapterEvent;
pub use protocol_group::{GroupMembershipEpoch, GroupProtocol};
pub use protocol_security::{
    AdapterSaslMechanism, AdapterSecurity, SASL_PASSWORD_ENVIRONMENT, SASL_USERNAME_ENVIRONMENT,
    TLS_CA_PEM_ENVIRONMENT, TerminalStatus,
};
pub use qualification::{
    QUALIFICATION_SCHEMA_VERSION, QualificationCell, QualificationError, QualificationManifest,
};
pub use qualification_evidence::{
    QUALIFICATION_EVIDENCE_SCHEMA_VERSION, QualificationCellEvidence, QualificationEvidenceError,
    QualificationEvidenceManifest, QualificationRunEvidence,
};
pub use record::{ConsumedRecord, HeaderSpec, RecordError, RecordSpec};
pub use scenario::{SCENARIO_SCHEMA_VERSION, Scenario};
pub use scenario_action::ScenarioAction;
pub use scenario_error::ScenarioError;
pub use scenario_types::{
    BatchRecord, BrokerBehavior, OperationAssertion, ScenarioStep, TransactionDisposition,
    VisibilityExpectation,
};
pub use share::{ShareConsumedRecord, ShareDisposition};
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
