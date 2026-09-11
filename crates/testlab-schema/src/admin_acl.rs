//! ACL administration retains literal wildcard-host bindings and positional public outcomes.

use serde::{Deserialize, Serialize};

use crate::{BrokerAclOperation, BrokerAclResource, ClientId, OperationId};

/// Permission decision for one concrete ACL binding.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AclPermission {
    /// Grants the selected operation.
    Allow,
    /// Rejects the selected operation.
    Deny,
}

/// One literal resource ACL whose Kafka host selector is exactly `*`.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiteralAclBinding {
    /// Literal Kafka resource.
    pub resource: BrokerAclResource,
    /// Exact Kafka principal, such as `User:alice`.
    pub principal: String,
    /// Exact Kafka operation.
    pub operation: BrokerAclOperation,
    /// Allow or deny decision.
    pub permission: AclPermission,
}

/// Scenario request for one caller-ordered ACL creation batch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateAclsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Caller-ordered concrete bindings.
    pub bindings: Vec<LiteralAclBinding>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one caller-ordered ACL creation batch.
pub type CreateAclsCommand = CreateAclsAction;

/// Scenario request for one exact ACL description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeAclsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Exact concrete binding used to construct the public filter.
    pub binding: LiteralAclBinding,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one exact ACL description.
pub type DescribeAclsCommand = DescribeAclsAction;

/// Scenario request for caller-ordered exact ACL deletion filters.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteAclsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Caller-ordered bindings converted to exact public filters.
    pub bindings: Vec<LiteralAclBinding>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for caller-ordered exact ACL deletion filters.
pub type DeleteAclsCommand = DeleteAclsAction;

/// Public result for one caller-ordered ACL creation batch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminAclsCreation {
    /// Stable identity of the completed public call.
    pub operation_id: OperationId,
    /// Caller-ordered per-binding outcomes.
    pub outcomes: Vec<AdminAclCreationOutcome>,
}

/// One public ACL creation outcome.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminAclCreationOutcome {
    /// Exact binding returned for this request position.
    pub binding: LiteralAclBinding,
    /// Normalized broker error, or none on success.
    pub error_code: Option<String>,
}

/// Public result for one exact ACL description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminAclsDescription {
    /// Stable identity of the completed public call.
    pub operation_id: OperationId,
    /// Deterministically ordered concrete bindings returned by Kafkars.
    pub bindings: Vec<LiteralAclBinding>,
}

/// Public result for caller-ordered exact ACL deletion filters.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminAclsDeletion {
    /// Stable identity of the completed public call.
    pub operation_id: OperationId,
    /// Caller-ordered per-filter outcomes.
    pub outcomes: Vec<AdminAclDeletionOutcome>,
}

/// One public ACL deletion filter outcome.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminAclDeletionOutcome {
    /// Exact binding used to construct this caller-position filter.
    pub filter: LiteralAclBinding,
    /// Filter-level normalized broker error, or none when Kafka evaluated it.
    pub error_code: Option<String>,
    /// Caller-ordered concrete matches returned for this filter.
    pub matches: Vec<AdminAclDeleteMatch>,
}

/// One concrete binding matched by an ACL deletion filter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminAclDeleteMatch {
    /// Exact concrete binding returned by Kafka.
    pub binding: LiteralAclBinding,
    /// Match-level normalized broker error, or none on deletion.
    pub error_code: Option<String>,
}

/// Independently observed presence of one exact ACL binding.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerAclState {
    /// Monotonic observation identity within the run.
    pub observation: u64,
    /// Admin operation whose public result triggered this observation.
    pub operation_id: OperationId,
    /// Exact literal wildcard-host ACL queried through Kafka's CLI.
    pub binding: LiteralAclBinding,
    /// Whether the exact binding was present.
    pub present: bool,
}
