//! Plural Share-group descriptions preserve caller order and detailed public outcomes.

use serde::{Deserialize, Serialize};

use crate::{AdminShareGroupDescription, ClientId, OperationId};

/// One scenario-side expectation for an active Share group.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ShareGroupDescriptionExpectation {
    /// Exact Kafka Share-group identity.
    pub group_id: String,
    /// Exact public and independently observed group state.
    pub expected_state: String,
    /// Exact public and independently observed member count.
    pub expected_member_count: u32,
    /// Exact public member rack identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_rack_id: Option<String>,
    /// Topic required in one public member subscription and assignment.
    pub expected_topic: String,
    /// Partition required in one public member assignment.
    pub expected_partition: i32,
}

/// Scenario intent for one caller-ordered Share-group description batch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeShareGroupsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-ordered active Share-group expectations.
    pub groups: Vec<ShareGroupDescriptionExpectation>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one caller-ordered Share-group description batch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeShareGroupsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-ordered exact Kafka Share-group identities.
    pub group_ids: Vec<String>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One detailed public Share-group description outcome.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminShareGroupDescriptionOutcome {
    /// Exact caller-selected Share-group identity.
    pub group_id: String,
    /// Detailed public description, absent when this group failed.
    pub description: Option<AdminShareGroupDescription>,
    /// Stable normalized per-group error code.
    pub error_code: Option<String>,
}

/// Public completion for one caller-ordered Share-group description batch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminShareGroupsDescription {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// One exact public outcome per requested group in caller order.
    pub outcomes: Vec<AdminShareGroupDescriptionOutcome>,
}
