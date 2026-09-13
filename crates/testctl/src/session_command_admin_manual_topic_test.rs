//! Manual topic translation tests preserve requested replica placement.

use testlab_schema::{
    AdapterCommand, ClientId, CreateTopicAction, OperationId, ScenarioAction,
    TopicReplicaAssignmentSpec,
};

#[test]
fn manual_topic_translation_preserves_exact_replica_assignments() {
    let assignments = vec![assignment(0, &[2, 1]), assignment(1, &[3, 2])];
    let action = ScenarioAction::CreateTopic(CreateTopicAction {
        client_id: ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}")),
        operation_id: OperationId::new("manual-create")
            .unwrap_or_else(|error| panic!("operation ID: {error}")),
        topic: "orders".to_owned(),
        partitions: 2,
        replication_factor: 2,
        replica_assignments: Some(assignments.clone()),
        configs: Vec::new(),
        validate_only: false,
        expected_error_code: None,
        timeout_ms: 1_000,
    });

    let (command, _) = crate::session_command_admin::translate(&action)
        .unwrap_or_else(|| panic!("manual create-topic translation"));
    let AdapterCommand::CreateTopic(command) = command else {
        panic!("create-topic command kind");
    };

    assert_eq!(command.replica_assignments, Some(assignments));
}

fn assignment(partition_index: i32, broker_ids: &[i32]) -> TopicReplicaAssignmentSpec {
    TopicReplicaAssignmentSpec {
        partition_index,
        broker_ids: broker_ids.to_vec(),
    }
}
