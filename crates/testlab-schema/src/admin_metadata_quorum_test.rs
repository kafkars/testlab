use crate::{
    AdapterCommand, AdapterEvent, AdminMetadataQuorumDescription, BrokerMetadataQuorumState,
    BrokerStateObservation, ClientId, DescribeMetadataQuorumAction, DescribeMetadataQuorumCommand,
    MetadataQuorumListenerState, MetadataQuorumNodeState, MetadataQuorumReplicaState, OperationId,
    ScenarioAction,
};

#[test]
fn metadata_quorum_protocol_and_evidence_round_trip() {
    let action = ScenarioAction::DescribeMetadataQuorum(DescribeMetadataQuorumAction {
        client_id: client(),
        operation_id: operation(),
        timeout_ms: 1_000,
    });
    let command = AdapterCommand::DescribeMetadataQuorum(DescribeMetadataQuorumCommand {
        client_id: client(),
        operation_id: operation(),
        timeout_ms: 1_000,
    });
    let description = AdminMetadataQuorumDescription {
        operation_id: operation(),
        leader_id: Some(1),
        leader_epoch: 2,
        high_watermark: 3,
        voters: vec![replica()],
        observers: Vec::new(),
        nodes: Some(vec![node()]),
    };
    round_trip(&action);
    round_trip(&command);
    round_trip(&AdapterEvent::MetadataQuorumDescribed(description));
    round_trip(&BrokerStateObservation::MetadataQuorum(
        BrokerMetadataQuorumState {
            observation: 7,
            operation_id: operation(),
            leader_id: Some(1),
            leader_epoch: 2,
            high_watermark: 3,
            voters: vec![replica()],
            observers: Vec::new(),
            nodes: vec![node()],
        },
    ));
}

fn replica() -> MetadataQuorumReplicaState {
    MetadataQuorumReplicaState {
        replica_id: 1,
        replica_directory_id: Some("AAECAwQFBgcICQoLDA0ODw".to_owned()),
        log_end_offset: Some(4),
        last_fetch_timestamp_ms: None,
        last_caught_up_timestamp_ms: None,
    }
}

fn node() -> MetadataQuorumNodeState {
    MetadataQuorumNodeState {
        node_id: 1,
        listeners: vec![MetadataQuorumListenerState {
            name: "CONTROLLER".to_owned(),
            host: "broker-1".to_owned(),
            port: 9093,
        }],
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("describe-metadata-quorum")
        .unwrap_or_else(|error| panic!("operation: {error}"))
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
