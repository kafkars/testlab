use testlab_schema::{
    AdapterCommand, ClientId, CreateTopicAction, OperationId, ScenarioAction,
    TopicReplicaAssignmentSpec,
};

use crate::observer_admin_target::AdminTarget;

#[test]
fn manual_topic_creation_maps_to_exact_assignment_observation() {
    let action = manual_action();
    let (command, target) = crate::observer_admin_topic_target::match_action(&action)
        .unwrap_or_else(|error| panic!("manual target: {error}"))
        .unwrap_or_else(|| panic!("manual target missing"));
    assert_eq!(
        AdminTarget::from_exact(&action, &command)
            .unwrap_or_else(|error| panic!("exact target: {error}")),
        Some(target.clone())
    );
    let AdminTarget::PartitionAssignments(target) = target else {
        panic!("manual topic assignment target kind");
    };
    assert_eq!(target.operation_id.as_str(), "manual-create");
    assert_eq!(target.assignments.len(), 2);
    assert_eq!(target.assignments[0].topic, "orders");
    assert_eq!(target.assignments[0].partition, 0);
    assert_eq!(target.assignments[0].replicas, vec![2, 1]);
    assert_eq!(target.assignments[1].partition, 1);
    assert_eq!(target.assignments[1].replicas, vec![3, 2]);
}

#[test]
fn manual_topic_creation_rejects_wire_assignment_drift() {
    let action = manual_action();
    let (mut command, _) = crate::observer_admin_topic_target::match_action(&action)
        .unwrap_or_else(|error| panic!("manual target: {error}"))
        .unwrap_or_else(|| panic!("manual target missing"));
    let AdapterCommand::CreateTopic(payload) = &mut command else {
        panic!("create-topic command kind");
    };
    payload
        .replica_assignments
        .as_mut()
        .unwrap_or_else(|| panic!("manual assignments"))[0]
        .broker_ids
        .reverse();

    assert!(AdminTarget::from_exact(&action, &command).is_err());
}

fn manual_action() -> ScenarioAction {
    ScenarioAction::CreateTopic(CreateTopicAction {
        client_id: ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}")),
        operation_id: OperationId::new("manual-create")
            .unwrap_or_else(|error| panic!("operation ID: {error}")),
        topic: "orders".to_owned(),
        partitions: 2,
        replication_factor: 2,
        replica_assignments: Some(vec![assignment(0, &[2, 1]), assignment(1, &[3, 2])]),
        configs: Vec::new(),
        validate_only: false,
        expected_error_code: None,
        timeout_ms: 1_000,
    })
}

fn assignment(partition_index: i32, broker_ids: &[i32]) -> TopicReplicaAssignmentSpec {
    TopicReplicaAssignmentSpec {
        partition_index,
        broker_ids: broker_ids.to_vec(),
    }
}
