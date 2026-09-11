use crate::{
    AdapterCommand, AdapterEvent, AdminBrokerLogDirsDescription, AdminLogDirDescription,
    AdminLogDirsDescription, BrokerLogDirState, BrokerLogDirsBrokerState, BrokerLogDirsState,
    BrokerStateObservation, ClientId, DescribeLogDirsAction, DescribeLogDirsCommand,
    LogDirReplicaState, OperationId, ScenarioAction,
};

#[test]
fn expectation_stays_off_wire_and_result_shapes_round_trip() {
    let action = ScenarioAction::DescribeLogDirs(DescribeLogDirsAction {
        client_id: client(),
        operation_id: operation(),
        topic: "orders".to_owned(),
        partition: 0,
        expected_replica_count: 1,
        timeout_ms: 1_000,
    });
    let command = AdapterCommand::DescribeLogDirs(DescribeLogDirsCommand {
        client_id: client(),
        operation_id: operation(),
        topic: "orders".to_owned(),
        partition: 0,
        timeout_ms: 1_000,
    });
    round_trip(&action);
    round_trip(&command);
    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode log-directory command: {error}"));
    assert!(!encoded.contains("expected_replica_count"));

    let replica = replica();
    round_trip(&AdapterEvent::LogDirsDescribed(AdminLogDirsDescription {
        operation_id: operation(),
        topic: "orders".to_owned(),
        partition: 0,
        throttle_time_ms: 7,
        brokers: vec![AdminBrokerLogDirsDescription {
            broker_id: 1,
            log_dirs: vec![AdminLogDirDescription {
                path: "/var/lib/kafka/data".to_owned(),
                total_bytes: Some(1_024),
                usable_bytes: Some(512),
                is_cordoned: Some(false),
                replicas: vec![replica.clone()],
            }],
        }],
    }));
    round_trip(&BrokerStateObservation::LogDirs(BrokerLogDirsState {
        observation: 3,
        operation_id: operation(),
        topic: "orders".to_owned(),
        partition: 0,
        brokers: vec![BrokerLogDirsBrokerState {
            broker_id: 1,
            log_dirs: vec![BrokerLogDirState {
                path: "/var/lib/kafka/data".to_owned(),
                replicas: vec![replica],
            }],
        }],
    }));
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

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-log-dirs").unwrap_or_else(|error| panic!("operation ID: {error}"))
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + Eq + std::fmt::Debug,
{
    let encoded = serde_json::to_vec(value)
        .unwrap_or_else(|error| panic!("serialize protocol value: {error}"));
    let decoded = serde_json::from_slice::<T>(&encoded)
        .unwrap_or_else(|error| panic!("deserialize protocol value: {error}"));
    assert_eq!(&decoded, value);
}
