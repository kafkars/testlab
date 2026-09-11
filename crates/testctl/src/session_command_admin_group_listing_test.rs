//! Generic group-listing translation keeps the selected public API on the wire.

use testlab_schema::{
    AdapterCommand, ClientId, GroupListingApi, ListConsumerGroupsAction, ListConsumerGroupsCommand,
    OperationId, ScenarioAction,
};

use crate::session_command_admin::translate;

#[test]
fn generic_group_listing_preserves_the_public_api_choice() {
    let client_id = id(ClientId::new("client-1"));
    let operation_id = id(OperationId::new("admin-list-all-groups-1"));
    let action = ScenarioAction::ListConsumerGroups(ListConsumerGroupsAction {
        client_id: client_id.clone(),
        operation_id: operation_id.clone(),
        api: GroupListingApi::AllGroups,
        required_group_ids: vec!["orders-group".to_owned()],
        timeout_ms: 20_000,
    });

    let Some((command, _)) = translate(&action) else {
        panic!("generic group listing must cross the adapter boundary");
    };

    assert_eq!(
        command,
        AdapterCommand::ListConsumerGroups(ListConsumerGroupsCommand {
            client_id,
            operation_id,
            api: GroupListingApi::AllGroups,
            timeout_ms: 20_000,
        })
    );
}

fn id<T, E: std::fmt::Display>(value: Result<T, E>) -> T {
    value.unwrap_or_else(|error| panic!("identifier: {error}"))
}
