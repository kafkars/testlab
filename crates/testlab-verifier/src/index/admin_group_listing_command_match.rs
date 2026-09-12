//! Group-listing command matching keeps every public filter exact.

use testlab_schema::{ListConsumerGroupsAction, ListConsumerGroupsCommand};

pub(super) fn matches(
    action: &ListConsumerGroupsAction,
    command: &ListConsumerGroupsCommand,
) -> bool {
    action.client_id == command.client_id
        && action.operation_id == command.operation_id
        && action.api == command.api
        && action.state_filters == command.state_filters
        && action.group_type_filters == command.group_type_filters
        && action.protocol_type_filters == command.protocol_type_filters
        && action.timeout_ms == command.timeout_ms
}
