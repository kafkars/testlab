//! Admin command routing composes request-family identity and exact-match helpers.

use testlab_schema::{AdapterCommand, OperationId, ScenarioAction};

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    super::admin_user_scram_command_match::action_operation_id(action)
        .or_else(|| super::admin_client_quota_command_match::action_operation_id(action))
        .or_else(|| super::admin_acl_command_match::action_operation_id(action))
        .or_else(|| super::admin_batch_command_match::action_operation_id(action))
        .or_else(|| super::admin_group_batch::action_operation_id(action))
        .or_else(|| super::admin_delete_records_command_match::action_operation_id(action))
        .or_else(|| super::admin_command_match::action_operation_id(action))
        .or_else(|| super::admin_config_command_match::action_operation_id(action))
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    super::admin_user_scram_command_match::command_operation_id(command)
        .or_else(|| super::admin_client_quota_command_match::command_operation_id(command))
        .or_else(|| super::admin_acl_command_match::command_operation_id(command))
        .or_else(|| super::admin_batch_command_match::command_operation_id(command))
        .or_else(|| super::admin_group_batch::command_operation_id(command))
        .or_else(|| super::admin_delete_records_command_match::command_operation_id(command))
        .or_else(|| super::admin_command_match::command_operation_id(command))
        .or_else(|| super::admin_config_command_match::command_operation_id(command))
}

pub(super) fn command_matches(action: &ScenarioAction, command: &AdapterCommand) -> bool {
    super::admin_user_scram_command_match::matches(action, command)
        .or_else(|| super::admin_client_quota_command_match::matches(action, command))
        .or_else(|| super::admin_acl_command_match::matches(action, command))
        .or_else(|| super::admin_batch_command_match::matches(action, command))
        .or_else(|| super::admin_group_batch::matches(action, command))
        .or_else(|| super::admin_delete_records_command_match::matches(action, command))
        .or_else(|| super::admin_config_command_match::matches(action, command))
        .unwrap_or_else(|| super::admin_command_match::matches(action, command))
}
