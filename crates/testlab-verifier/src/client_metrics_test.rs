//! Client metrics tests cover exact ownership, coherence, and declared producer state.

use testlab_schema::{
    AdapterCommand, AdapterEvent, CallMetricsSnapshot, ClientMetricsObservation,
    ClientMetricsSnapshot, FailureMetricsSnapshot, LatencyMetricSnapshot, LatencyMetricsSnapshot,
    MailboxMetricsSnapshot, ObserveClientMetricsCommand, ProducerMetricsSnapshot, Scenario,
    ScenarioAction,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn coherent_snapshot_satisfies_all_metrics_contracts() {
    assert!(verify(snapshot()).is_empty());
}

#[test]
fn incoherent_and_under_floor_snapshots_fail_distinct_contracts() {
    let mut value = snapshot();
    value.producer.produce_requests = 2;
    value.producer.produce_records = 0;
    let violations = verify(value);
    assert!(has(&violations, "METRICS-002"));
    assert!(has(&violations, "METRICS-003"));
}

fn verify(snapshot: ClientMetricsSnapshot) -> Vec<testlab_schema::Violation> {
    let scenario = scenario();
    let ScenarioAction::ObserveClientMetrics(action) = &scenario.steps[5].action else {
        panic!("client metrics action missing");
    };
    let history = vec![
        command(
            1,
            AdapterCommand::ObserveClientMetrics(ObserveClientMetricsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
            }),
        ),
        event(
            2,
            AdapterEvent::ClientMetricsObserved(Box::new(ClientMetricsObservation {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                snapshot,
            })),
        ),
    ];
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    crate::client_metrics::verify(&scenario, &index, &mut violations);
    violations
}

fn snapshot() -> ClientMetricsSnapshot {
    ClientMetricsSnapshot {
        calls: CallMetricsSnapshot {
            admitted: 2,
            succeeded: 2,
            failed: 0,
            observer_abandoned: 0,
            not_sent: 0,
            possibly_sent: 0,
        },
        failures: FailureMetricsSnapshot {
            dns: 0,
            connect: 0,
            transport: 0,
            negotiation: 0,
            authentication: 0,
            deadline: 0,
            local_rejection: 0,
            response_capacity: 0,
            route_capacity: 0,
        },
        mailbox: MailboxMetricsSnapshot {
            capacity_per_lane: 1_024,
            byte_capacity_per_lane: 1_048_576,
            queued_work: 0,
            queued_work_bytes: 0,
            queued_control: 0,
            queued_control_bytes: 0,
            work_full: 0,
            work_byte_full: 0,
            control_full: 0,
            control_byte_full: 0,
            closed_rejections: 0,
            wake_failures: 0,
        },
        latency: LatencyMetricsSnapshot {
            mailbox: latency(2),
            routing: latency(2),
            preparation: latency(2),
            writer_admission: latency(2),
            in_flight: latency(2),
            end_to_end: latency(2),
            deadline_lateness: latency(0),
        },
        producer: ProducerMetricsSnapshot {
            active_records: 0,
            active_bytes: 0,
            waiting_records: 0,
            waiting_bytes: 0,
            prepared_batches: 0,
            prepared_batch_bytes: 0,
            terminal_backlog: 0,
            produce_requests: 1,
            produce_batches: 1,
            produce_records: 1,
            produce_encoded_bytes: 32,
            peak_produce_in_flight_requests: 1,
            peak_produce_in_flight_requests_per_broker: 1,
            accepting: true,
            healthy: true,
        },
    }
}

fn latency(samples: u64) -> LatencyMetricSnapshot {
    LatencyMetricSnapshot {
        samples,
        total_ns: samples.saturating_mul(10),
        max_ns: u64::from(samples > 0) * 10,
    }
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/client-metrics-producer.toml"
    ))
    .unwrap_or_else(|error| panic!("parse client metrics: {error}"))
}

fn has(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
