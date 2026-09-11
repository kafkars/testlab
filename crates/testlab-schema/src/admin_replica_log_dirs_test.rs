use crate::{
    AdapterCommand, AdapterEvent, AdminReplicaLogDirDescription, AdminReplicaLogDirsDescription,
    ClientId, DescribeReplicaLogDirsAction, DescribeReplicaLogDirsCommand, OperationId,
    ReplicaLogDirIdentity, ReplicaLogDirLocationState, ScenarioAction,
};

#[test]
fn expectation_stays_off_wire_and_public_placements_round_trip() {
    let action = ScenarioAction::DescribeReplicaLogDirs(DescribeReplicaLogDirsAction {
        client_id: client(),
        operation_id: operation(),
        topic: "orders".to_owned(),
        partition: 0,
        expected_replica_count: 1,
        timeout_ms: 1_000,
    });
    let command = AdapterCommand::DescribeReplicaLogDirs(DescribeReplicaLogDirsCommand {
        client_id: client(),
        operation_id: operation(),
        topic: "orders".to_owned(),
        partition: 0,
        timeout_ms: 1_000,
    });
    round_trip(&action);
    round_trip(&command);
    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode replica log-directory command: {error}"));
    assert!(!encoded.contains("expected_replica_count"));

    round_trip(&AdapterEvent::ReplicaLogDirsDescribed(
        AdminReplicaLogDirsDescription {
            operation_id: operation(),
            topic: "orders".to_owned(),
            partition: 0,
            throttle_time_ms: 7,
            replicas: vec![AdminReplicaLogDirDescription {
                replica: ReplicaLogDirIdentity {
                    topic: "orders".to_owned(),
                    partition: 0,
                    broker_id: 1,
                },
                current: Some(ReplicaLogDirLocationState {
                    path: "/var/lib/kafka/data".to_owned(),
                    offset_lag: -1,
                }),
                future: None,
            }],
        },
    ));
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-replica-log-dirs")
        .unwrap_or_else(|error| panic!("operation ID: {error}"))
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
