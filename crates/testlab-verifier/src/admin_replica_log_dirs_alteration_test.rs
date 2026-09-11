//! Replica log-directory alteration verdicts pin outcomes, convergence, and timing.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminReplicaLogDirAlterationOutcome,
    AdminReplicaLogDirsAlteration, AlterReplicaLogDirsAction, AlterReplicaLogDirsCommand,
    BrokerLogDirState, BrokerLogDirsBrokerState, BrokerLogDirsState, BrokerStateObservation,
    HistoryEntry, HistoryPayload, LogDirReplicaState, OperationId, ReplicaLogDirAssignmentSpec,
    ReplicaLogDirIdentity, ScenarioAction, TerminalStatus, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn exact_caller_ordered_alteration_and_settled_cli_state_pass() {
    assert!(violations(history()).is_empty());
}

#[test]
fn outcome_order_path_future_and_timing_mismatches_fail() {
    let mut reordered = history();
    alteration(&mut reordered).outcomes.reverse();
    assert_contract(&violations(reordered));

    let mut wrong_path = history();
    log_dirs(&mut wrong_path).brokers[0].log_dirs.swap(0, 1);
    assert_contract(&violations(wrong_path));

    let mut future = history();
    log_dirs(&mut future).brokers[0].log_dirs[1].replicas[0].is_future = true;
    assert_contract(&violations(future));

    let mut delayed = history();
    delayed.insert(2, command(2, AdapterCommand::Finish));
    delayed[3].sequence = 3;
    assert_contract(&violations(delayed));
}

fn history() -> Vec<HistoryEntry> {
    let assignments = assignments();
    vec![
        command(
            0,
            AdapterCommand::AlterReplicaLogDirs(AlterReplicaLogDirsCommand {
                client_id: client(),
                operation_id: operation(),
                assignments: assignments.clone(),
                timeout_ms: 20_000,
            }),
        ),
        event(
            1,
            AdapterEvent::ReplicaLogDirsAltered(AdminReplicaLogDirsAlteration {
                operation_id: operation(),
                throttle_time_ms: 3,
                outcomes: assignments.iter().map(outcome).collect(),
            }),
        ),
        state(2),
    ]
}

fn violations(history: Vec<HistoryEntry>) -> Vec<testlab_schema::Violation> {
    let mut fixture = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    fixture.steps.insert(
        2,
        step(
            "alter-replica-log-dirs",
            ScenarioAction::AlterReplicaLogDirs(AlterReplicaLogDirsAction {
                client_id: client(),
                operation_id: operation(),
                assignments: assignments(),
                timeout_ms: 20_000,
            }),
        ),
    );
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    verify_admin(&fixture, &index, &[], &mut violations);
    violations
}

fn assignments() -> Vec<ReplicaLogDirAssignmentSpec> {
    vec![assignment(3), assignment(1)]
}

fn assignment(broker_id: i32) -> ReplicaLogDirAssignmentSpec {
    ReplicaLogDirAssignmentSpec {
        topic: "orders".to_owned(),
        partition: 0,
        broker_id,
        target_path: "/data-secondary".to_owned(),
    }
}

fn outcome(assignment: &ReplicaLogDirAssignmentSpec) -> AdminReplicaLogDirAlterationOutcome {
    AdminReplicaLogDirAlterationOutcome {
        replica: ReplicaLogDirIdentity {
            topic: assignment.topic.clone(),
            partition: assignment.partition,
            broker_id: assignment.broker_id,
        },
        error_code: None,
    }
}

fn state(sequence: u64) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::LogDirs(BrokerLogDirsState {
                observation: 7,
                operation_id: operation(),
                topic: "orders".to_owned(),
                partition: 0,
                brokers: vec![broker(1), broker(3)],
            }),
        },
    }
}

fn broker(broker_id: i32) -> BrokerLogDirsBrokerState {
    BrokerLogDirsBrokerState {
        broker_id,
        log_dirs: vec![
            BrokerLogDirState {
                path: "/data-primary".to_owned(),
                replicas: Vec::new(),
            },
            BrokerLogDirState {
                path: "/data-secondary".to_owned(),
                replicas: vec![LogDirReplicaState {
                    topic: "orders".to_owned(),
                    partition: 0,
                    size_bytes: 42,
                    offset_lag: 0,
                    is_future: false,
                }],
            },
        ],
    }
}

fn alteration(entries: &mut [HistoryEntry]) -> &mut AdminReplicaLogDirsAlteration {
    let HistoryPayload::AdapterEvent { event } = &mut entries[1].payload else {
        panic!("adapter event");
    };
    let AdapterEvent::ReplicaLogDirsAltered(value) = &mut event.event else {
        panic!("replica log-directory alteration");
    };
    value
}

fn log_dirs(entries: &mut [HistoryEntry]) -> &mut BrokerLogDirsState {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[2].payload else {
        panic!("broker state");
    };
    let BrokerStateObservation::LogDirs(value) = observation else {
        panic!("log-directory state");
    };
    value
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-alter-replica-log-dirs")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-070"),
        "{violations:?}"
    );
}
