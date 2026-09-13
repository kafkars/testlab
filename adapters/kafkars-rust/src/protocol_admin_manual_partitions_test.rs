//! Manual partition-expansion adapter tests pin exact replica assignments.

use testlab_schema::{ClientId, CreatePartitionsCommand, OperationId};

#[test]
fn manual_assignments_select_the_public_partition_assignment_builder() {
    let command = command(Some(vec![vec![3, 1], vec![2, 3]]));

    let partitions = crate::protocol_admin_write::new_partitions(&command);

    assert_eq!(partitions.topic(), "orders");
    assert_eq!(partitions.total_count(), 3);
    assert_eq!(
        partitions.replica_assignments(),
        Some([vec![3, 1], vec![2, 3]].as_slice())
    );
}

#[test]
fn absent_assignments_retain_automatic_partition_placement() {
    let command = command(None);

    let partitions = crate::protocol_admin_write::new_partitions(&command);

    assert_eq!(partitions.topic(), "orders");
    assert_eq!(partitions.total_count(), 3);
    assert_eq!(partitions.replica_assignments(), None);
}

fn command(replica_assignments: Option<Vec<Vec<i32>>>) -> CreatePartitionsCommand {
    CreatePartitionsCommand {
        client_id: ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}")),
        operation_id: OperationId::new("manual-expand")
            .unwrap_or_else(|error| panic!("operation ID: {error}")),
        topic: "orders".to_owned(),
        total_count: 3,
        replica_assignments,
        validate_only: false,
        timeout_ms: 1_000,
    }
}
