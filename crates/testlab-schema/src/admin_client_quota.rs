//! Client-quota administration retains one exact named-user byte-rate operation.

use serde::{Deserialize, Serialize};

use crate::{BrokerQuotaDirection, ClientId, OperationId};

/// Scenario request for one exact named-user byte-rate replacement or removal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlterClientQuotaAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Exact non-default Kafka user quota entity.
    pub user: String,
    /// Producer or consumer byte-rate key.
    pub direction: BrokerQuotaDirection,
    /// Replacement rate, or none to remove the override.
    pub bytes_per_second: Option<u64>,
    /// Whether Kafka validates the alteration without changing quota state.
    pub validate_only: bool,
    /// Exact current rate required after successful validation-only completion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_current_bytes_per_second: Option<u64>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one exact named-user byte-rate replacement or removal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlterClientQuotaCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Exact non-default Kafka user quota entity.
    pub user: String,
    /// Producer or consumer byte-rate key.
    pub direction: BrokerQuotaDirection,
    /// Replacement rate, or none to remove the override.
    pub bytes_per_second: Option<u64>,
    /// Whether Kafka validates the alteration without changing quota state.
    pub validate_only: bool,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Scenario request for one exact named-user byte-rate description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeClientQuotaAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Exact non-default Kafka user quota entity.
    pub user: String,
    /// Producer or consumer byte-rate key.
    pub direction: BrokerQuotaDirection,
    /// Whether entities with unspecified component types are excluded.
    #[serde(default = "default_strict_filter")]
    pub strict: bool,
    /// Exact whole-number value required by the verifier.
    pub expected_bytes_per_second: u64,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one exact named-user byte-rate description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeClientQuotaCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Exact non-default Kafka user quota entity.
    pub user: String,
    /// Producer or consumer byte-rate key.
    pub direction: BrokerQuotaDirection,
    /// Whether entities with unspecified component types are excluded.
    pub strict: bool,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

const fn default_strict_filter() -> bool {
    true
}

/// Public completion for one exact named-user quota replacement or removal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminClientQuotaAlteration {
    /// Stable identity of the completed public call.
    pub operation_id: OperationId,
    /// Exact named-user entity returned by the public batch result.
    pub user: String,
    /// Exact key changed through the public request.
    pub direction: BrokerQuotaDirection,
}

/// Public result for one exact named-user quota description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminClientQuotaDescription {
    /// Stable identity of the completed public call.
    pub operation_id: OperationId,
    /// Exact named-user entity returned by the public result.
    pub user: String,
    /// Exact key returned by the public result.
    pub direction: BrokerQuotaDirection,
    /// Exact whole-number byte rate returned by the public result.
    pub bytes_per_second: u64,
}

/// Independently observed state of one exact named-user byte-rate quota.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerClientQuotaState {
    /// Monotonic observation identity within the run.
    pub observation: u64,
    /// Admin operation whose public result triggered this observation.
    pub operation_id: OperationId,
    /// Exact non-default Kafka user quota entity queried through Kafka's CLI.
    pub user: String,
    /// Producer or consumer byte-rate key.
    pub direction: BrokerQuotaDirection,
    /// Exact current rate, or none when the override is absent.
    pub bytes_per_second: Option<u64>,
}
