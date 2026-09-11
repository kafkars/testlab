use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminBrokerLogDirsDescription, AdminLogDirDescription,
    AdminLogDirsDescription, BrokerLogDirState, BrokerLogDirsBrokerState, BrokerLogDirsState,
    BrokerStateObservation, DescribeLogDirsAction, DescribeLogDirsCommand, HistoryEntry,
    HistoryPayload, LogDirReplicaState, OperationId, ScenarioAction, TerminalStatus,
    VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn descending_public_brokers_match_canonical_cli_state() {
    assert!(violations(history(), 1).is_empty());
}

#[test]
fn count_order_shared_fields_and_timing_mismatches_fail() {
    assert_contract(&violations(history(), 2));

    let mut wrong_order = history();
    let HistoryPayload::AdapterEvent { event } = &mut wrong_order[1].payload else {
        panic!("public log-directory event");
    };
    let AdapterEvent::LogDirsDescribed(public) = &mut event.event else {
        panic!("public log-directory description");
    };
    public.brokers.reverse();
    assert_contract(&violations(wrong_order, 1));

    let mut wrong_size = history();
    let HistoryPayload::BrokerStateObservation { observation } = &mut wrong_size[2].payload else {
        panic!("independent log-directory state");
    };
    let BrokerStateObservation::LogDirs(independent) = observation else {
        panic!("log-directory observation kind");
    };
    independent.brokers[0].log_dirs[0].replicas[0].size_bytes = 99;
    assert_contract(&violations(wrong_size, 1));

    let mut delayed = history();
    delayed.insert(2, command(2, AdapterCommand::Finish));
    delayed[3].sequence = 3;
    delayed[3].observed_unix_ms = 3;
    assert_contract(&violations(delayed, 1));
}

fn history() -> Vec<HistoryEntry> {
    let operation_id = operation();
    let replica = replica();
    vec![
        command(
            0,
            AdapterCommand::DescribeLogDirs(DescribeLogDirsCommand {
                client_id: client(),
                operation_id: operation_id.clone(),
                topic: "orders".to_owned(),
                partition: 0,
                timeout_ms: 1_000,
            }),
        ),
        event(
            1,
            AdapterEvent::LogDirsDescribed(AdminLogDirsDescription {
                operation_id: operation_id.clone(),
                topic: "orders".to_owned(),
                partition: 0,
                throttle_time_ms: 3,
                brokers: vec![
                    public_broker(2, "/data-b", None),
                    public_broker(1, "/data-a", Some(replica.clone())),
                ],
            }),
        ),
        state(
            2,
            BrokerStateObservation::LogDirs(BrokerLogDirsState {
                observation: 4,
                operation_id,
                topic: "orders".to_owned(),
                partition: 0,
                brokers: vec![
                    broker_state(1, "/data-a", Some(replica)),
                    broker_state(2, "/data-b", None),
                ],
            }),
        ),
    ]
}

fn violations(history: Vec<HistoryEntry>, expected_count: usize) -> Vec<testlab_schema::Violation> {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.steps.insert(
        2,
        step(
            "describe-log-dirs",
            ScenarioAction::DescribeLogDirs(DescribeLogDirsAction {
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

fn public_broker(
    broker_id: i32,
    path: &str,
    replica: Option<LogDirReplicaState>,
) -> AdminBrokerLogDirsDescription {
    AdminBrokerLogDirsDescription {
        broker_id,
        log_dirs: vec![AdminLogDirDescription {
            path: path.to_owned(),
            total_bytes: Some(1_024),
            usable_bytes: Some(512),
            is_cordoned: Some(false),
            replicas: replica.into_iter().collect(),
        }],
    }
}

fn broker_state(
    broker_id: i32,
    path: &str,
    replica: Option<LogDirReplicaState>,
) -> BrokerLogDirsBrokerState {
    BrokerLogDirsBrokerState {
        broker_id,
        log_dirs: vec![BrokerLogDirState {
            path: path.to_owned(),
            replicas: replica.into_iter().collect(),
        }],
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
    OperationId::new("admin-describe-log-dirs")
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-054"),
        "{violations:?}"
    );
}
