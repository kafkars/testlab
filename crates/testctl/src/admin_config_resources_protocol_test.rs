//! Generic configuration-resource protocol tests keep expectations off the wire.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminConfigResource, AdminConfigResourcesListing, ClientId,
    ListConfigResourcesAction, OperationId, ScenarioAction,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn translation_and_completion_preserve_the_operation_identity() {
    let action = ScenarioAction::ListConfigResources(ListConfigResourcesAction {
        client_id: client(),
        operation_id: operation(),
        required_topics: vec!["topic-z".to_owned(), "topic-a".to_owned()],
        timeout_ms: 1_000,
    });
    let Some((AdapterCommand::ListConfigResources(command), expected)) =
        crate::session_command_admin_config::translate(&action)
    else {
        panic!("configuration-resource translation");
    };
    assert_eq!(command.client_id, client());
    assert_eq!(command.operation_id, operation());
    assert_eq!(command.timeout_ms, 1_000);
    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode resource command: {error}"));
    assert!(!encoded.contains("topic-z"), "{encoded}");
    assert_eq!(
        expected
            .classify(&AdapterEvent::ConfigResourcesListed(listing()))
            .unwrap_or_else(|error| panic!("classify resource listing: {error}")),
        EventDisposition::Complete
    );
    assert!(matches!(expected, ExpectedEvent::ConfigResourcesListed(_)));
}

fn listing() -> AdminConfigResourcesListing {
    AdminConfigResourcesListing {
        operation_id: operation(),
        throttle_time_ms: 0,
        resources: vec![AdminConfigResource {
            resource_type: 2,
            name: "topic-a".to_owned(),
        }],
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("list-config-resources").unwrap_or_else(|error| panic!("operation: {error}"))
}
