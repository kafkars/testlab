//! User SCRAM administration retains exact non-secret credential metadata.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// SCRAM mechanism selected by one credential operation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScramCredentialMechanism {
    /// SCRAM using SHA-256.
    Sha256,
    /// SCRAM using SHA-512.
    Sha512,
}

impl ScramCredentialMechanism {
    /// Returns Kafka's canonical configuration spelling.
    pub const fn config_name(self) -> &'static str {
        match self {
            Self::Sha256 => "SCRAM-SHA-256",
            Self::Sha512 => "SCRAM-SHA-512",
        }
    }
}

/// Scenario request for one named-user SCRAM upsert or deletion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlterUserScramCredentialAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Exact non-default Kafka user.
    pub user: String,
    /// Credential mechanism to replace or delete.
    pub mechanism: ScramCredentialMechanism,
    /// Upsert iterations, or none to delete this credential.
    ///
    /// Upserts resolve password bytes from [`crate::SASL_PASSWORD_ENVIRONMENT`]
    /// inside the adapter process, so secrets never enter protocol history.
    pub iterations: Option<u32>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one named-user SCRAM upsert or deletion.
pub type AlterUserScramCredentialCommand = AlterUserScramCredentialAction;

/// Scenario request for one exact named-user SCRAM description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeUserScramCredentialAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Exact non-default Kafka user.
    pub user: String,
    /// Credential mechanism selected from the user's result.
    pub mechanism: ScramCredentialMechanism,
    /// Exact expected iterations, or none for resource-not-found absence.
    pub expected_iterations: Option<u32>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one exact named-user SCRAM description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeUserScramCredentialCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Exact non-default Kafka user.
    pub user: String,
    /// Credential mechanism selected from the user's result.
    pub mechanism: ScramCredentialMechanism,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Public completion for one named-user SCRAM upsert or deletion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminUserScramCredentialAlteration {
    /// Stable identity of the completed public call.
    pub operation_id: OperationId,
    /// Exact named user returned by the public batch result.
    pub user: String,
    /// Credential mechanism changed by the exact public request.
    pub mechanism: ScramCredentialMechanism,
}

/// Public result for one named-user SCRAM description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminUserScramCredentialDescription {
    /// Stable identity of the completed public call.
    pub operation_id: OperationId,
    /// Exact named user returned by the public result.
    pub user: String,
    /// Credential mechanism selected from the public result.
    pub mechanism: ScramCredentialMechanism,
    /// Exact public iteration count, or none for resource-not-found absence.
    pub iterations: Option<u32>,
    /// Exact public broker code establishing absence, otherwise none.
    pub absence_broker_code: Option<i16>,
}

/// Independently observed state of one named-user SCRAM credential.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerUserScramCredentialState {
    /// Monotonic observation identity within the run.
    pub observation: u64,
    /// Admin operation whose public result triggered this observation.
    pub operation_id: OperationId,
    /// Exact non-default Kafka user queried through Kafka's CLI.
    pub user: String,
    /// Exact credential mechanism queried through Kafka's CLI.
    pub mechanism: ScramCredentialMechanism,
    /// Exact non-secret iteration count, or none when the credential is absent.
    pub iterations: Option<u32>,
}
