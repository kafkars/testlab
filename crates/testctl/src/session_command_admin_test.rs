//! Admin command translation tests preserve requested intent and event identity.

use testlab_schema::{AdapterCommand, ClientId, OperationId, ScenarioAction};

use crate::session_command_admin::translate;

#[test]
fn partition_creation_translation_preserves_requested_total() {
    let client_id = id(ClientId::new("client-1"));
    let operation_id = id(OperationId::new("admin-partitions-1"));
    let action = ScenarioAction::CreatePartitions {
        client_id: client_id.clone(),
        operation_id: operation_id.clone(),
        topic: "orders".to_owned(),
        total_count: 3,
        timeout_ms: 20_000,
    };

    let Some((command, _)) = translate(&action) else {
        panic!("partition creation must cross the adapter boundary");
    };

    assert_eq!(
        command,
        AdapterCommand::CreatePartitions {
            client_id,
            operation_id,
            topic: "orders".to_owned(),
            total_count: 3,
            timeout_ms: 20_000,
        }
    );
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
