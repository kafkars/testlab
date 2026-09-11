//! Share-group lifecycle protocol types preserve caller order and per-group outcomes.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// Scenario intent for deleting multiple empty Share groups through one public call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteShareGroupsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-ordered distinct Share-group identities.
    pub group_ids: Vec<String>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for deleting multiple empty Share groups through one public call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteShareGroupsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-ordered distinct Share-group identities.
    pub group_ids: Vec<String>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One public Share-group deletion outcome.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminShareGroupDeletionOutcome {
    /// Exact Share-group identity returned by the public client.
    pub group_id: String,
    /// Stable normalized per-group error code.
    pub error_code: Option<String>,
}

/// Caller-ordered public completion for multiple Share-group deletions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminShareGroupsDeletion {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// One exact outcome per requested Share group in caller order.
    pub outcomes: Vec<AdminShareGroupDeletionOutcome>,
}
