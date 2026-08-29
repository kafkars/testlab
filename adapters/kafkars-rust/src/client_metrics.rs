//! Client metrics normalize every public Kafkars snapshot getter into protocol evidence.

use std::time::{Duration, Instant};

use testlab_schema::{
    CallMetricsSnapshot, ClientId, ClientMetricsObservation, ClientMetricsSnapshot,
    FailureMetricsSnapshot, LatencyMetricSnapshot, LatencyMetricsSnapshot, MailboxMetricsSnapshot,
    OperationId, ProducerMetricsSnapshot,
};

use crate::admission_retry::retry_until;
use crate::kafkars_api::{ErrorKind, KafkarsLatencyMetric, KafkarsMetricsSnapshot};
use crate::state::{AdapterState, StateError};

impl AdapterState {
    pub(crate) fn observe_client_metrics(
        &self,
        client_id: ClientId,
        operation_id: OperationId,
    ) -> Result<ClientMetricsObservation, StateError> {
        let client = self.client(&client_id)?;
        let started = Instant::now();
        let deadline = started
            .checked_add(Duration::from_secs(30))
            .unwrap_or(started);
        let snapshot = retry_until(
            deadline,
            || client.metrics(),
            |error| error.kind() == ErrorKind::Backpressure,
        )
        .map_err(StateError::Client)?
        .wait()
        .map_err(StateError::Client)?;
        Ok(ClientMetricsObservation {
            client_id,
            operation_id,
            snapshot: normalize(&snapshot),
        })
    }
}

fn normalize(snapshot: &KafkarsMetricsSnapshot) -> ClientMetricsSnapshot {
    let calls = snapshot.calls();
    let failures = snapshot.failures();
    let mailbox = snapshot.mailbox();
    let latency = snapshot.latency();
    let producer = snapshot.producer();
    ClientMetricsSnapshot {
        calls: CallMetricsSnapshot {
            admitted: calls.admitted(),
            succeeded: calls.succeeded(),
            failed: calls.failed(),
            observer_abandoned: calls.observer_abandoned(),
            not_sent: calls.not_sent(),
            possibly_sent: calls.possibly_sent(),
        },
        failures: FailureMetricsSnapshot {
            dns: failures.dns(),
            connect: failures.connect(),
            transport: failures.transport(),
            negotiation: failures.negotiation(),
            authentication: failures.authentication(),
            deadline: failures.deadline(),
            local_rejection: failures.local_rejection(),
            response_capacity: failures.response_capacity(),
            route_capacity: failures.route_capacity(),
        },
        mailbox: MailboxMetricsSnapshot {
            capacity_per_lane: mailbox.capacity_per_lane(),
            byte_capacity_per_lane: mailbox.byte_capacity_per_lane(),
            queued_work: mailbox.queued_work(),
            queued_work_bytes: mailbox.queued_work_bytes(),
            queued_control: mailbox.queued_control(),
            queued_control_bytes: mailbox.queued_control_bytes(),
            work_full: mailbox.work_full(),
            work_byte_full: mailbox.work_byte_full(),
            control_full: mailbox.control_full(),
            control_byte_full: mailbox.control_byte_full(),
            closed_rejections: mailbox.closed_rejections(),
            wake_failures: mailbox.wake_failures(),
        },
        latency: LatencyMetricsSnapshot {
            mailbox: normalize_latency(latency.mailbox()),
            routing: normalize_latency(latency.routing()),
            preparation: normalize_latency(latency.preparation()),
            writer_admission: normalize_latency(latency.writer_admission()),
            in_flight: normalize_latency(latency.in_flight()),
            end_to_end: normalize_latency(latency.end_to_end()),
            deadline_lateness: normalize_latency(latency.deadline_lateness()),
        },
        producer: ProducerMetricsSnapshot {
            active_records: producer.active_records(),
            active_bytes: producer.active_bytes(),
            waiting_records: producer.waiting_records(),
            waiting_bytes: producer.waiting_bytes(),
            prepared_batches: producer.prepared_batches(),
            prepared_batch_bytes: producer.prepared_batch_bytes(),
            terminal_backlog: producer.terminal_backlog(),
            produce_requests: producer.produce_requests(),
            produce_batches: producer.produce_batches(),
            produce_records: producer.produce_records(),
            produce_encoded_bytes: producer.produce_encoded_bytes(),
            peak_produce_in_flight_requests: producer.peak_produce_in_flight_requests(),
            peak_produce_in_flight_requests_per_broker: producer
                .peak_produce_in_flight_requests_per_broker(),
            accepting: producer.accepting(),
            healthy: producer.healthy(),
        },
    }
}

fn normalize_latency(metric: KafkarsLatencyMetric) -> LatencyMetricSnapshot {
    LatencyMetricSnapshot {
        samples: metric.samples(),
        total_ns: duration_ns(metric.total()),
        max_ns: duration_ns(metric.max()),
    }
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
