//! Client metrics preserve public snapshot facts without exposing scenario expectations.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// Scenario expectations for one bounded public client metrics snapshot.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObserveClientMetricsAction {
    /// Existing client identity.
    pub client_id: ClientId,
    /// Stable snapshot operation identity.
    pub operation_id: OperationId,
    /// Smallest acceptable cumulative produced-record count.
    pub minimum_produce_records: u64,
    /// Requires all current producer ownership gauges to be empty.
    pub require_idle_producer: bool,
    /// Required public producer admission state.
    pub require_accepting: bool,
    /// Required public producer host health state.
    pub require_healthy: bool,
}

/// Expectation-free command for one public client metrics snapshot.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObserveClientMetricsCommand {
    /// Existing client identity.
    pub client_id: ClientId,
    /// Stable snapshot operation identity.
    pub operation_id: OperationId,
}

/// One normalized public client metrics observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientMetricsObservation {
    /// Observed client identity.
    pub client_id: ClientId,
    /// Stable snapshot operation identity.
    pub operation_id: OperationId,
    /// Complete public snapshot.
    pub snapshot: ClientMetricsSnapshot,
}

/// Complete normalized view of every public metrics family.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientMetricsSnapshot {
    /// Broker-call lifecycle counters.
    pub calls: CallMetricsSnapshot,
    /// Classified broker-call failures.
    pub failures: FailureMetricsSnapshot,
    /// Driver mailbox pressure.
    pub mailbox: MailboxMetricsSnapshot,
    /// Broker-call latency stages.
    pub latency: LatencyMetricsSnapshot,
    /// Producer ownership and throughput.
    pub producer: ProducerMetricsSnapshot,
}

/// Cumulative public broker-call lifecycle counters.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CallMetricsSnapshot {
    /// Calls accepted for interpretation.
    pub admitted: u64,
    /// Calls completed successfully.
    pub succeeded: u64,
    /// Calls completed with typed failures.
    pub failed: u64,
    /// Terminal values discarded after observer abandonment.
    pub observer_abandoned: u64,
    /// Failures known not to have crossed transport ownership.
    pub not_sent: u64,
    /// Failures whose requests may have reached Kafka.
    pub possibly_sent: u64,
}

/// Cumulative public broker-call failure classifications.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FailureMetricsSnapshot {
    /// Broker-name resolution failures.
    pub dns: u64,
    /// Transport-establishment failures.
    pub connect: u64,
    /// Established-transport losses.
    pub transport: u64,
    /// API negotiation failures.
    pub negotiation: u64,
    /// Authentication failures.
    pub authentication: u64,
    /// Absolute-deadline failures.
    pub deadline: u64,
    /// Local validation, preparation, or writer rejections.
    pub local_rejection: u64,
    /// Response-registry capacity rejections.
    pub response_capacity: u64,
    /// Route, query, or coordinator capacity rejections.
    pub route_capacity: u64,
}

/// Public bounded driver-mailbox gauges and rejection counters.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MailboxMetricsSnapshot {
    /// Independent work and control command bound.
    pub capacity_per_lane: usize,
    /// Independent work and control retained-byte bound.
    pub byte_capacity_per_lane: usize,
    /// Ordinary commands awaiting interpretation.
    pub queued_work: usize,
    /// Bytes retained by ordinary queued commands.
    pub queued_work_bytes: usize,
    /// Priority control commands awaiting interpretation.
    pub queued_control: usize,
    /// Bytes retained by priority queued commands.
    pub queued_control_bytes: usize,
    /// Cumulative ordinary count-capacity rejections.
    pub work_full: u64,
    /// Cumulative ordinary byte-capacity rejections.
    pub work_byte_full: u64,
    /// Cumulative control count-capacity rejections.
    pub control_full: u64,
    /// Cumulative control byte-capacity rejections.
    pub control_byte_full: u64,
    /// Commands rejected after driver admission closed.
    pub closed_rejections: u64,
    /// Admitted commands returned after poller wake failure.
    pub wake_failures: u64,
}

/// Public stage-latency summaries.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LatencyMetricsSnapshot {
    /// Submission-to-reactor-admission latency.
    pub mailbox: LatencyMetricSnapshot,
    /// Reactor-admission-to-route latency.
    pub routing: LatencyMetricSnapshot,
    /// Route-to-frame-preparation latency.
    pub preparation: LatencyMetricSnapshot,
    /// Preparation-to-writer-admission latency.
    pub writer_admission: LatencyMetricSnapshot,
    /// Writer-admission-to-terminal latency.
    pub in_flight: LatencyMetricSnapshot,
    /// Public submission-to-terminal latency.
    pub end_to_end: LatencyMetricSnapshot,
    /// Deadline settlement lateness.
    pub deadline_lateness: LatencyMetricSnapshot,
}

/// Count and nanosecond totals for one public latency stage.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LatencyMetricSnapshot {
    /// Completed observations.
    pub samples: u64,
    /// Saturating total duration in nanoseconds.
    pub total_ns: u64,
    /// Largest duration in nanoseconds.
    pub max_ns: u64,
}

/// Public producer ownership, throughput, concurrency, and health metrics.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProducerMetricsSnapshot {
    /// Application records retained by active ownership.
    pub active_records: usize,
    /// Application bytes retained by active ownership.
    pub active_bytes: usize,
    /// Records retained in bounded waiting ownership.
    pub waiting_records: usize,
    /// Bytes retained in bounded waiting ownership.
    pub waiting_bytes: usize,
    /// Protocol-materialized batches retained for execution.
    pub prepared_batches: usize,
    /// Encoded bytes retained by prepared batches.
    pub prepared_batch_bytes: usize,
    /// Terminal decisions awaiting completion publication.
    pub terminal_backlog: usize,
    /// Cumulative driver-accepted Produce requests.
    pub produce_requests: u64,
    /// Cumulative partition batches in Produce requests.
    pub produce_batches: u64,
    /// Cumulative records in Produce requests.
    pub produce_records: u64,
    /// Cumulative encoded record bytes in Produce requests.
    pub produce_encoded_bytes: u64,
    /// Peak Produce requests owned by transport.
    pub peak_produce_in_flight_requests: usize,
    /// Peak Produce requests owned by one broker connection.
    pub peak_produce_in_flight_requests_per_broker: usize,
    /// Whether the producer accepted records at capture.
    pub accepting: bool,
    /// Whether the producer host retained healthy ownership.
    pub healthy: bool,
}
