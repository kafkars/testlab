use testlab_schema::{
    AdapterCommand, ClientId, CreatePartitionsAction, OperationId, ScenarioAction,
};

use crate::observer_admin_target::AdminTarget;

#[test]
fn manual_partition_expansion_maps_new_partition_offsets_to_exact_indices() {
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
        panic!("manual partition assignment target kind");
    };
    assert_eq!(target.operation_id.as_str(), "manual-expand");
    assert_eq!(target.assignments.len(), 2);
    assert_eq!(target.assignments[0].topic, "orders");
    assert_eq!(target.assignments[0].partition, 1);
    assert_eq!(target.assignments[0].replicas, vec![3, 1]);
    assert_eq!(target.assignments[1].partition, 2);
    assert_eq!(target.assignments[1].replicas, vec![2, 3]);
}

#[test]
fn manual_partition_expansion_rejects_wire_assignment_drift() {
    let action = manual_action();
    let (mut command, _) = crate::observer_admin_topic_target::match_action(&action)
        .unwrap_or_else(|error| panic!("manual target: {error}"))
        .unwrap_or_else(|| panic!("manual target missing"));
    let AdapterCommand::CreatePartitions(payload) = &mut command else {
        panic!("create-partitions command kind");
    };
    payload
        .replica_assignments
        .as_mut()
        .unwrap_or_else(|| panic!("manual assignments"))[0]
        .reverse();

    assert!(AdminTarget::from_exact(&action, &command).is_err());
}

fn manual_action() -> ScenarioAction {
    ScenarioAction::CreatePartitions(CreatePartitionsAction {
        client_id: ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}")),
        operation_id: OperationId::new("manual-expand")
            .unwrap_or_else(|error| panic!("operation ID: {error}")),
        topic: "orders".to_owned(),
        total_count: 3,
        replica_assignments: Some(vec![vec![3, 1], vec![2, 3]]),
        validate_only: false,
        expected_current_count: Some(1),
        expected_error_code: None,
        timeout_ms: 1_000,
    })
}
