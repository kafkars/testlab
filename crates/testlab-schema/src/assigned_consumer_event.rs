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
    /// The caller-supplied position deadline elapsed.
    DeadlineElapsed,
    /// The client driver rejected the position request.
    DriverRejected,
    /// The transport failed before a valid broker response was available.
    Transport,
    /// The broker rejected the position request.
    Broker {
        /// Exact Kafka protocol error code returned by the broker.
        code: i16,
    },
    /// The broker response was incompatible with the negotiated protocol.
    Compatibility,
    /// The broker response could not be validated.
    InvalidResponse,
    /// The broker response exceeded the client's configured bound.
    ResponseTooLarge,
    /// The broker throttle could not fit within the remaining deadline.
    ThrottleDeadlineOverflow,
}

/// Stable public reason the next Fetch could not be scheduled.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignedConsumerFetchThrottleFailure {
    /// The required throttle delay could not fit within the Fetch deadline.
    DeadlineOverflow,
}

/// Stable public reason one exact Fetch terminated.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "failure_kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AssignedConsumerFetchFailure {
    /// The caller-supplied Fetch deadline elapsed.
    DeadlineElapsed,
    /// The client driver rejected the Fetch request.
    DriverRejected,
    /// The transport failed before a valid broker response was available.
    Transport,
    /// The broker rejected the Fetch request.
    Broker {
        /// Exact Kafka protocol error code returned by the broker.
        code: i16,
    },
    /// The broker response was incompatible with the negotiated protocol.
    Compatibility,
    /// The broker response could not be validated.
    InvalidResponse,
    /// The broker response exceeded the client's configured bound.
    ResponseTooLarge,
}

/// Public position fence observed from the packaged client.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssignedConsumerPositionFenceObservation {
    /// Public topic whose position was being resolved.
    pub topic: String,
    /// Public partition whose position was being resolved.
    pub partition: i32,
    /// Assignment generation retained when position resolution began.
    pub assignment_epoch: u64,
    /// Position request generation retained by the public event.
    pub position_epoch: u64,
}

/// Public Fetch fence observed from the packaged client.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssignedConsumerFetchFenceObservation {
    /// Position fence used by the failed Fetch.
    pub position: AssignedConsumerPositionFenceObservation,
    /// Fetch generation retained by the public event.
    pub fetch_revision: u64,
}

/// Exact normalized public event returned by the adapter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "event_kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AssignedConsumerEventObservationKind {
    /// One position resolution terminated with an exact public failure.
    PositionResolutionFailed {
        /// Public position identity and generation retained at failure.
        fence: AssignedConsumerPositionFenceObservation,
        /// Normalized public position failure.
        failure: AssignedConsumerPositionFailure,
    },
    /// Scheduling the next Fetch failed while applying a broker throttle.
    FetchThrottleFailed {
        /// Public Fetch identity and generations retained at failure.
        fence: AssignedConsumerFetchFenceObservation,
        /// Normalized public throttle failure.
        failure: AssignedConsumerFetchThrottleFailure,
    },
    /// One exact Fetch terminated with a public failure.
    FetchFailed {
        /// Public Fetch identity and generations retained at failure.
        fence: AssignedConsumerFetchFenceObservation,
        /// Normalized public Fetch failure.
        failure: AssignedConsumerFetchFailure,
    },
}

/// One completed direct-consumer event observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssignedConsumerEventObservation {
    /// Stable observation identity copied from the command.
    pub operation_id: OperationId,
    /// Direct consumer that produced the retained event.
    pub consumer_id: ConsumerId,
    /// Exact public observer used to retrieve the event.
    pub method: AssignedConsumerEventMethod,
    /// Exact normalized public event returned by the adapter.
    pub event: AssignedConsumerEventObservationKind,
}

#[cfg(test)]
#[path = "assigned_consumer_event_test.rs"]
mod tests;
