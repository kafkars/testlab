//! Small JSON Lines wire contract isolates harness self-tests from client claims.

use serde::{Deserialize, Serialize};
use testlab_schema::{OperationId, RecordSpec};

/// One record request sent by the reference adapter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelBrokerRequest {
    /// Stable public operation identity.
    pub operation_id: OperationId,
    /// Record bytes presented to the broker fixture.
    pub record: RecordSpec,
}

/// One model-broker response.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelBrokerResponse {
    /// Response classification.
    pub status: ModelBrokerResponseStatus,
    /// Assigned offset for an acknowledgment.
    pub offset: Option<i64>,
    /// Stable rejection code.
    pub code: Option<String>,
}

/// Model-broker terminal response classification.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelBrokerResponseStatus {
    /// The fixture persisted and acknowledged the record.
    Acknowledged,
    /// The fixture rejected the record before persistence.
    Rejected,
}
