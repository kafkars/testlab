use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminReplicaLogDirDescription, AdminReplicaLogDirsDescription,
    BrokerLogDirState, BrokerLogDirsBrokerState, BrokerLogDirsState, BrokerStateObservation,
    DescribeReplicaLogDirsAction, DescribeReplicaLogDirsCommand, HistoryEntry, HistoryPayload,
    LogDirReplicaState, OperationId, ReplicaLogDirIdentity, ReplicaLogDirLocationState,
    ScenarioAction, TerminalStatus, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn descending_public_brokers_match_canonical_cli_placements() {
    assert!(violations(history(), 1).is_empty());
}

#[test]
fn count_order_lag_future_and_timing_mismatches_fail() {
    assert_contract(&violations(history(), 2));

    let mut wrong_order = history();
    let HistoryPayload::AdapterEvent { event } = &mut wrong_order[1].payload else {
        panic!("public replica log-directory event");
    };
    let AdapterEvent::ReplicaLogDirsDescribed(public) = &mut event.event else {
        panic!("public replica log-directory description");
    };
    public.replicas.reverse();
    assert_contract(&violations(wrong_order, 1));

    let mut wrong_lag = history();
    let HistoryPayload::BrokerStateObservation { observation } = &mut wrong_lag[2].payload else {
        panic!("independent log-directory state");
    };
    let BrokerStateObservation::LogDirs(independent) = observation else {
        panic!("log-directory observation kind");
    };
    independent.brokers[0].log_dirs[0].replicas[0].offset_lag = 9;
    assert_contract(&violations(wrong_lag, 1));

    let mut unexpected_future = history();
    let HistoryPayload::AdapterEvent { event } = &mut unexpected_future[1].payload else {
        panic!("public replica log-directory event");
    };
    let AdapterEvent::ReplicaLogDirsDescribed(public) = &mut event.event else {
        panic!("public replica log-directory description");
    };
    public.replicas[1].future = public.replicas[1].current.clone();
    assert_contract(&violations(unexpected_future, 1));

    let mut delayed = history();
    delayed.insert(2, command(2, AdapterCommand::Finish));
    delayed[3].sequence = 3;
    delayed[3].observed_unix_ms = 3;
    assert_contract(&violations(delayed, 1));
}

fn history() -> Vec<HistoryEntry> {
    let operation_id = operation();
    vec![
        command(
            0,
            AdapterCommand::DescribeReplicaLogDirs(DescribeReplicaLogDirsCommand {
                client_id: client(),
                operation_id: operation_id.clone(),
                topic: "orders".to_owned(),
                partition: 0,
                timeout_ms: 1_000,
            }),
        ),
        event(
            1,
            AdapterEvent::ReplicaLogDirsDescribed(AdminReplicaLogDirsDescription {
                operation_id: operation_id.clone(),
                topic: "orders".to_owned(),
                partition: 0,
                throttle_time_ms: 3,
                replicas: vec![public(2, None), public(1, Some(location()))],
            }),
        ),
        state(
            2,
            BrokerStateObservation::LogDirs(BrokerLogDirsState {
                observation: 4,
                operation_id,
                topic: "orders".to_owned(),
                partition: 0,
                brokers: vec![broker(1, Some(replica())), broker(2, None)],
            }),
        ),
    ]
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the test helper owns each constructed history fixture"
)]
fn violations(history: Vec<HistoryEntry>, expected_count: usize) -> Vec<testlab_schema::Violation> {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.steps.insert(
        2,
        step(
            "describe-replica-log-dirs",
            ScenarioAction::DescribeReplicaLogDirs(DescribeReplicaLogDirsAction {
                client_id: client(),
                operation_id: operation(),
                topic: "orders".to_owned(),
                partition: 0,
                expected_replica_count: expected_count,
                timeout_ms: 1_000,
            }),
        ),
    );
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    verify_admin(&scenario, &index, &[], &mut violations);
    violations
}

fn public(
    broker_id: i32,
    current: Option<ReplicaLogDirLocationState>,
) -> AdminReplicaLogDirDescription {
    AdminReplicaLogDirDescription {
        replica: ReplicaLogDirIdentity {
            topic: "orders".to_owned(),
            partition: 0,
            broker_id,
        },
        current,
        future: None,
    }
}

fn broker(broker_id: i32, replica: Option<LogDirReplicaState>) -> BrokerLogDirsBrokerState {
    BrokerLogDirsBrokerState {
        broker_id,
        log_dirs: vec![BrokerLogDirState {
            path: format!("/data-{broker_id}"),
            replicas: replica.into_iter().collect(),
        }],
    }
}

fn location() -> ReplicaLogDirLocationState {
    ReplicaLogDirLocationState {
        path: "/data-1".to_owned(),
        offset_lag: 0,
    }
}

fn replica() -> LogDirReplicaState {
    LogDirReplicaState {
        topic: "orders".to_owned(),
        partition: 0,
        size_bytes: 42,
        offset_lag: 0,
        is_future: false,
    }
}

fn state(sequence: u64, observation: BrokerStateObservation) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation { observation },
    }
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-describe-replica-log-dirs")
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-055"),
        "{violations:?}"
    );
}
