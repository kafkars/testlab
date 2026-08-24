//! Partition-administration wire tests separate requested intent from public completion.

use super::{AdapterCommand, AdapterEvent, ClientId, OperationId};

#[test]
fn create_partitions_command_carries_requested_total() {
    let command = AdapterCommand::CreatePartitions {
        client_id: client("client-1"),
        operation_id: operation("admin-partitions-1"),
        topic: "records".to_owned(),
        total_count: 2,
        timeout_ms: 1_000,
    };

    let encoded = toml::to_string(&command)
        .unwrap_or_else(|error| panic!("serialize create-partitions command: {error}"));

    assert!(encoded.contains("kind = \"create_partitions\""));
    assert!(encoded.contains("total_count = 2"));
}

#[test]
fn partition_completion_reports_only_operation_and_topic() {
    let event = AdapterEvent::TopicPartitionsCreated {
        operation_id: operation("admin-partitions-1"),
        topic: "records".to_owned(),
    };

    let encoded = toml::to_string(&event)
        .unwrap_or_else(|error| panic!("serialize partition completion: {error}"));

    assert!(encoded.contains("kind = \"topic_partitions_created\""));
    assert!(encoded.contains("operation_id = \"admin-partitions-1\""));
    assert!(encoded.contains("topic = \"records\""));
    assert!(!encoded.contains("total_count"));
}

fn client(value: &str) -> ClientId {
    ClientId::new(value).unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
