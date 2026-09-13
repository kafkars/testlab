use testlab_schema::{ClientId, CreatePartitionsAction, OperationId, ScenarioAction};

#[test]
fn manual_partition_translation_preserves_exact_replica_assignments() {
    let assignments = vec![vec![3, 1], vec![2, 3]];
    let action = ScenarioAction::CreatePartitions(CreatePartitionsAction {
        client_id: ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}")),
        operation_id: OperationId::new("manual-expand")
            .unwrap_or_else(|error| panic!("operation ID: {error}")),
        topic: "orders".to_owned(),
        total_count: 3,
        replica_assignments: Some(assignments.clone()),
        validate_only: false,
        expected_current_count: Some(1),
        expected_error_code: None,
        timeout_ms: 1_000,
    });

    let (command, _) = crate::session_command_admin::translate(&action)
        .unwrap_or_else(|| panic!("manual create-partitions translation"));
    let testlab_schema::AdapterCommand::CreatePartitions(command) = command else {
        panic!("create-partitions command kind");
    };

    assert_eq!(command.replica_assignments, Some(assignments));
}
