//! Partition-reassignment protocol tests pin command and event identities.

use testlab_schema::{
    AdapterCommand, AlterPartitionReassignmentsAction, ClientId, ListPartitionReassignmentsAction,
    OperationId, PartitionReassignmentChangeSpec, ScenarioAction,
};

use crate::runner_protocol::ExpectedEvent;

#[test]
fn reassignment_expectations_do_not_cross_the_wire() {
    let alter = ScenarioAction::AlterPartitionReassignments(AlterPartitionReassignmentsAction {
        client_id: client(),
        operation_id: operation("alter"),
        changes: vec![PartitionReassignmentChangeSpec {
            topic: "orders".to_owned(),
            partition: 1,
            replicas: vec![2, 1],
        }],
        allow_replication_factor_change: true,
        timeout_ms: 20_000,
    });
    let Some((AdapterCommand::AlterPartitionReassignments(command), expected)) =
        crate::session_command_admin_partition_reassignments::translate(&alter)
    else {
        panic!("expected reassignment mutation translation");
    };
    assert_eq!(command.changes.len(), 1);
    assert!(matches!(
        expected,
        ExpectedEvent::PartitionReassignmentsAltered(_)
    ));

    let list = ScenarioAction::ListPartitionReassignments(ListPartitionReassignmentsAction {
        client_id: client(),
        operation_id: operation("list"),
        targets: None,
        expected_reassignments: Vec::new(),
        timeout_ms: 20_000,
    });
    let Some((AdapterCommand::ListPartitionReassignments(command), expected)) =
        crate::session_command_admin_partition_reassignments::translate(&list)
    else {
        panic!("expected reassignment listing translation");
    };
    assert!(command.targets.is_none());
    assert!(matches!(
        expected,
        ExpectedEvent::PartitionReassignmentsListed(_)
    ));
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(format!("reassignments-{value}"))
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}
