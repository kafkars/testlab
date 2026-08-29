//! Client metrics verification checks identity, public counter coherence, and declared state.

use testlab_schema::{
    ClientMetricsSnapshot, LatencyMetricSnapshot, ObserveClientMetricsAction, Scenario,
    ScenarioAction, Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for step in &scenario.steps {
        let ScenarioAction::ObserveClientMetrics(action) = &step.action else {
            continue;
        };
        let observations = index.client_metrics.get(&action.operation_id);
        let count = observations.map_or(0, Vec::len);
        let exact = index.action_issued(&step.action)
            && count == 1
            && observations
                .and_then(|values| values.first())
                .is_some_and(|indexed| {
                    indexed.observation.client_id == action.client_id
                        && indexed.observation.operation_id == action.operation_id
                });
        if !exact {
            violations.push(violation(
                "METRICS-001",
                format!(
                    "metrics operation {} expected one issued command and exact client {}, observed {count} snapshot(s)",
                    action.operation_id, action.client_id
                ),
                Some(action.operation_id.clone()),
                references(observations),
            ));
            continue;
        }
        let Some(indexed) = observations.and_then(|values| values.first()) else {
            continue;
        };
        verify_coherence(
            &action.operation_id,
            &indexed.observation.snapshot,
            indexed.history_sequence,
            violations,
        );
        verify_expectations(
            action,
            &indexed.observation.snapshot,
            indexed.history_sequence,
            violations,
        );
    }
}

fn verify_coherence(
    operation_id: &testlab_schema::OperationId,
    snapshot: &ClientMetricsSnapshot,
    sequence: u64,
    violations: &mut Vec<Violation>,
) {
    let calls = &snapshot.calls;
    let terminal_calls = calls.succeeded.saturating_add(calls.failed);
    let delivery_failures = calls.not_sent.saturating_add(calls.possibly_sent);
    let classified_failures = failure_total(snapshot);
    let mailbox = &snapshot.mailbox;
    let producer = &snapshot.producer;
    let latency_valid = latency_metrics(snapshot)
        .into_iter()
        .all(|metric| latency_metric_valid(&metric));
    let coherent = terminal_calls <= calls.admitted
        && delivery_failures <= calls.failed
        && classified_failures <= calls.failed
        && snapshot.latency.end_to_end.samples <= terminal_calls
        && mailbox.capacity_per_lane > 0
        && mailbox.byte_capacity_per_lane > 0
        && mailbox.queued_work <= mailbox.capacity_per_lane
        && mailbox.queued_control <= mailbox.capacity_per_lane
        && mailbox.queued_work_bytes <= mailbox.byte_capacity_per_lane
        && mailbox.queued_control_bytes <= mailbox.byte_capacity_per_lane
        && producer.produce_requests <= producer.produce_batches
        && producer.produce_batches <= producer.produce_records
        && producer.peak_produce_in_flight_requests_per_broker
            <= producer.peak_produce_in_flight_requests
        && latency_valid;
    if !coherent {
        violations.push(violation(
            "METRICS-002",
            "public metrics snapshot violates cumulative, capacity, latency, or producer ordering invariants".to_owned(),
            Some(operation_id.clone()),
            vec![format!("history:{sequence}")],
        ));
    }
}

fn verify_expectations(
    action: &ObserveClientMetricsAction,
    snapshot: &ClientMetricsSnapshot,
    sequence: u64,
    violations: &mut Vec<Violation>,
) {
    let producer = &snapshot.producer;
    let idle = producer.active_records == 0
        && producer.active_bytes == 0
        && producer.waiting_records == 0
        && producer.waiting_bytes == 0
        && producer.prepared_batches == 0
        && producer.prepared_batch_bytes == 0
        && producer.terminal_backlog == 0;
    let matches = producer.produce_records >= action.minimum_produce_records
        && (!action.require_idle_producer || idle)
        && producer.accepting == action.require_accepting
        && producer.healthy == action.require_healthy;
    if !matches {
        violations.push(violation(
            "METRICS-003",
            format!(
                "expected at least {} produced record(s), idle={}, accepting={}, healthy={}; observed records={}, idle={idle}, accepting={}, healthy={}",
                action.minimum_produce_records,
                action.require_idle_producer,
                action.require_accepting,
                action.require_healthy,
                producer.produce_records,
                producer.accepting,
                producer.healthy
            ),
            Some(action.operation_id.clone()),
            vec![format!("history:{sequence}")],
        ));
    }
}

fn failure_total(snapshot: &ClientMetricsSnapshot) -> u64 {
    let failure = &snapshot.failures;
    [
        failure.dns,
        failure.connect,
        failure.transport,
        failure.negotiation,
        failure.authentication,
        failure.deadline,
        failure.local_rejection,
        failure.response_capacity,
        failure.route_capacity,
    ]
    .into_iter()
    .fold(0_u64, u64::saturating_add)
}

fn latency_metrics(snapshot: &ClientMetricsSnapshot) -> [LatencyMetricSnapshot; 7] {
    let latency = snapshot.latency;
    [
        latency.mailbox,
        latency.routing,
        latency.preparation,
        latency.writer_admission,
        latency.in_flight,
        latency.end_to_end,
        latency.deadline_lateness,
    ]
}

fn latency_metric_valid(metric: &LatencyMetricSnapshot) -> bool {
    metric.max_ns <= metric.total_ns
        && (metric.samples > 0 || (metric.total_ns == 0 && metric.max_ns == 0))
}

fn references(observations: Option<&Vec<crate::index::IndexedClientMetrics>>) -> Vec<String> {
    observations
        .into_iter()
        .flatten()
        .map(|value| format!("history:{}", value.history_sequence))
        .collect()
}
