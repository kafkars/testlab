//! Unsupported-command tests keep capability classification explicit.

use testlab_schema::{AdapterCommand, AdminOffsetPosition, ClientId, ConsumerId, OperationId};

use crate::session_unsupported::reason;

#[test]
fn read_only_admin_commands_require_admin_capability() {
    let client_id = client_id();
    let operation_id = operation_id();
    let commands = [
        AdapterCommand::DescribeTopic {
            client_id: client_id.clone(),
            operation_id: operation_id.clone(),
            topic: "orders".to_owned(),
            timeout_ms: 1_000,
        },
        AdapterCommand::ListTopics {
            client_id: client_id.clone(),
            operation_id: operation_id.clone(),
            include_internal: false,
            timeout_ms: 1_000,
        },
        AdapterCommand::ListOffsets {
            client_id,
            operation_id,
            topic: "orders".to_owned(),
            partition: 0,
            position: AdminOffsetPosition::Latest,
            timeout_ms: 1_000,
        },
    ];

    for command in commands {
        assert_eq!(reason(&command), "admin capability required");
    }
}

#[test]
fn share_commands_require_share_consumer_capability() {
    let command = AdapterCommand::CreateShareConsumer {
        client_id: client_id(),
        consumer_id: ConsumerId::new("share-1")
            .unwrap_or_else(|error| panic!("consumer id: {error}")),
        group_id: "share-group".to_owned(),
        topic: "orders".to_owned(),
        membership_timeout_ms: 1_000,
        close_timeout_ms: 1_000,
    };

    assert_eq!(reason(&command), "share_consumer capability required");
}

fn client_id() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation_id() -> OperationId {
    OperationId::new("admin-read-1").unwrap_or_else(|error| panic!("operation id: {error}"))
}
