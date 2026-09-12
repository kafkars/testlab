//! Direct-consumer event contracts preserve public failure identity across the adapter boundary.

use serde::{Deserialize, Serialize};

use crate::{ConsumerId, OperationId};

/// Public observer selected for one retained assigned-consumer event.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignedConsumerEventMethod {
    /// Wait on the named retained-event observer.
    #[default]
    NextEvent,
    /// Repeatedly attempt the immediate retained-event take operation.
    TryTakeEvent,
}

/// One bounded public observation with an independently declared expected event.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObserveAssignedConsumerEventAction {
    /// Stable observation identity.
    pub operation_id: OperationId,
    /// Existing directly assigned consumer.
    pub consumer_id: ConsumerId,
    /// Exact public event observer.
    #[serde(default)]
    pub method: AssignedConsumerEventMethod,
    /// Exact normalized failure expected from the public event.
    pub expected: AssignedConsumerEventExpectation,
    /// Complete observation bound supplied by Testlab.
    pub timeout_ms: u64,
}

/// Adapter command stripped of the scenario's expected result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObserveAssignedConsumerEventCommand {
    /// Stable observation identity.
    pub operation_id: OperationId,
    /// Existing directly assigned consumer.
    pub consumer_id: ConsumerId,
    /// Exact public event observer.
    pub method: AssignedConsumerEventMethod,
    /// Complete observation bound supplied by Testlab.
    pub timeout_ms: u64,
}

/// Scenario-side failure identity without implementation-owned fence generations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "event_kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AssignedConsumerEventExpectation {
    /// One exact position resolution must fail.
    PositionResolutionFailed {
        /// Exact topic retained by the public fence.
        topic: String,
        /// Exact partition retained by the public fence.
        partition: i32,
        /// Exact public position-resolution category.
        failure: AssignedConsumerPositionFailure,
    },
    /// Scheduling after one successful Fetch must fail.
    FetchThrottleFailed {
        /// Exact topic retained by the public fence.
        topic: String,
        /// Exact partition retained by the public fence.
        partition: i32,
        /// Exact public throttle category.
        failure: AssignedConsumerFetchThrottleFailure,
    },
    /// One exact Fetch execution must fail.
    FetchFailed {
        /// Exact topic retained by the public fence.
        topic: String,
        /// Exact partition retained by the public fence.
        partition: i32,
        /// Exact public Fetch category.
        failure: AssignedConsumerFetchFailure,
    },
}

/// Stable public reason one position resolution terminated.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "failure_kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AssignedConsumerPositionFailure {
    DeadlineElapsed,
    DriverRejected,
    Transport,
    Broker { code: i16 },
    Compatibility,
    InvalidResponse,
    ResponseTooLarge,
    ThrottleDeadlineOverflow,
}

/// Stable public reason the next Fetch could not be scheduled.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignedConsumerFetchThrottleFailure {
    DeadlineOverflow,
}

/// Stable public reason one exact Fetch terminated.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "failure_kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AssignedConsumerFetchFailure {
    DeadlineElapsed,
    DriverRejected,
    Transport,
    Broker { code: i16 },
    Compatibility,
    InvalidResponse,
    ResponseTooLarge,
}

/// Public position fence observed from the packaged client.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssignedConsumerPositionFenceObservation {
    pub topic: String,
    pub partition: i32,
    pub assignment_epoch: u64,
    pub position_epoch: u64,
}

/// Public Fetch fence observed from the packaged client.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssignedConsumerFetchFenceObservation {
    pub position: AssignedConsumerPositionFenceObservation,
    pub fetch_revision: u64,
}

/// Exact normalized public event returned by the adapter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "event_kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AssignedConsumerEventObservationKind {
    PositionResolutionFailed {
        fence: AssignedConsumerPositionFenceObservation,
        failure: AssignedConsumerPositionFailure,
    },
    FetchThrottleFailed {
        fence: AssignedConsumerFetchFenceObservation,
        failure: AssignedConsumerFetchThrottleFailure,
    },
    FetchFailed {
        fence: AssignedConsumerFetchFenceObservation,
        failure: AssignedConsumerFetchFailure,
    },
}

/// One completed direct-consumer event observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssignedConsumerEventObservation {
    pub operation_id: OperationId,
    pub consumer_id: ConsumerId,
    pub method: AssignedConsumerEventMethod,
    pub event: AssignedConsumerEventObservationKind,
}

#[cfg(test)]
#[path = "assigned_consumer_event_test.rs"]
mod tests;
