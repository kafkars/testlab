//! Batch offset target tests pin exact wire matching and observation order.

use testlab_schema::{
    AdapterCommand, AdminOffsetPosition, ClientId, ListOffsetsBatchAction, ListOffsetsBatchCommand,
    OffsetListingExpectation, OffsetListingSelection, OperationId, ScenarioAction,
};

use crate::observer_admin_target::AdminTarget;

#[test]
fn batch_offset_action_maps_to_one_exact_ordered_target() {
    let action = action();
    let command = command();

    let target = AdminTarget::from_exact(&action, &command)
        .unwrap_or_else(|error| panic!("match batch offsets: {error}"))
        .unwrap_or_else(|| panic!("missing batch offset target"));

    let AdminTarget::PartitionOffsetsBatch(target) = target else {
        panic!("unexpected target kind");
    };
    assert_eq!(target.offsets.len(), 2);
    assert_eq!(target.offsets[0].partition, 2);
    assert_eq!(target.offsets[0].expected_high, Some(5));
    assert_eq!(target.offsets[1].partition, 0);
    assert_eq!(target.offsets[1].expected_low, Some(0));
}

#[test]
fn batch_offset_target_rejects_a_reordered_wire_command() {
    let action = action();
    let AdapterCommand::ListOffsetsBatch(mut command) = command() else {
        panic!("command changed shape");
    };
    command.queries.swap(0, 1);

    assert!(AdminTarget::from_exact(&action, &AdapterCommand::ListOffsetsBatch(command)).is_err());
}

fn action() -> ScenarioAction {
    ScenarioAction::ListOffsetsBatch(ListOffsetsBatchAction {
        client_id: client(),
        operation_id: operation(),
        queries: vec![
            expectation(2, AdminOffsetPosition::Latest, 5),
            expectation(0, AdminOffsetPosition::Earliest, 0),
        ],
        timeout_ms: 1_000,
    })
}

fn command() -> AdapterCommand {
    AdapterCommand::ListOffsetsBatch(ListOffsetsBatchCommand {
        client_id: client(),
        operation_id: operation(),
        queries: vec![
            selection(2, AdminOffsetPosition::Latest),
            selection(0, AdminOffsetPosition::Earliest),
        ],
        timeout_ms: 1_000,
    })
}

fn expectation(
    partition: i32,
    position: AdminOffsetPosition,
    expected_offset: i64,
) -> OffsetListingExpectation {
    OffsetListingExpectation {
        topic: "records".to_owned(),
        partition,
        position,
        expected_offset,
    }
}

fn selection(partition: i32, position: AdminOffsetPosition) -> OffsetListingSelection {
    OffsetListingSelection {
        topic: "records".to_owned(),
        partition,
        position,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-offsets-batch").unwrap_or_else(|error| panic!("operation: {error}"))
}
