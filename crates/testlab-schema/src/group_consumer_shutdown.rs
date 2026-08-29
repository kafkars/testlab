//! Group-consumer shutdown preserves repeated public requests and terminal observation.

use serde::{Deserialize, Serialize};

use crate::{ConsumerId, OperationId};

/// Scenario request for clone-shared group shutdown and public stream termination.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupConsumerShutdownAction {
    /// Stable public operation identity.
    pub operation_id: OperationId,
    /// Existing hosted group consumer consumed by this terminal operation.
    pub consumer_id: ConsumerId,
    /// Number of idempotent public shutdown requests to issue.
    pub request_count: u8,
    /// Complete request and public termination-observation bound.
    pub timeout_ms: u64,
}

/// Expectation-free wire request for one hosted group shutdown.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupConsumerShutdownCommand {
    /// Stable public operation identity.
    pub operation_id: OperationId,
    /// Existing hosted group consumer consumed by this terminal operation.
    pub consumer_id: ConsumerId,
    /// Number of idempotent public shutdown requests to issue.
    pub request_count: u8,
    /// Complete request and public termination-observation bound.
    pub timeout_ms: u64,
}

/// Public event-stream termination after clone-shared shutdown requests.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupConsumerShutdownCompletion {
    /// Stable public operation identity.
    pub operation_id: OperationId,
    /// Hosted group consumer whose public event stream terminated.
    pub consumer_id: ConsumerId,
    /// Exact number of public shutdown requests issued.
    pub request_count: u8,
}
