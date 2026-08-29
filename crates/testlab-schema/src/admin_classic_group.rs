//! Classic-group batch descriptions keep expected membership off the wire.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// One scenario-side classic-group membership expectation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClassicGroupExpectation {
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Exact public member count required by the scenario.
    pub expected_member_count: u32,
}

/// Scenario intent for one bounded classic-group batch description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeClassicGroupsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-ordered classic-group expectations.
    pub groups: Vec<ClassicGroupExpectation>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded classic-group batch description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeClassicGroupsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-ordered exact Kafka consumer-group identities.
    pub group_ids: Vec<String>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One public classic-group description outcome.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminClassicGroupDescriptionOutcome {
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Public member count, absent when this group failed.
    pub member_count: Option<u32>,
    /// Stable normalized per-group error code.
    pub error_code: Option<String>,
}

/// Public result for one classic-group batch description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminClassicGroupsDescription {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-ordered public group outcomes.
    pub outcomes: Vec<AdminClassicGroupDescriptionOutcome>,
}
