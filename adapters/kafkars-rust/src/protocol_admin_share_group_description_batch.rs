//! Plural Share-group description retains caller order and detailed per-group results.

use std::io::Write;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminShareGroupDescriptionOutcome,
    AdminShareGroupsDescription, CommandId, DescribeShareGroupsCommand,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::protocol::emit;
use crate::protocol_admin_plural_result::{ResourceResult, ordered_group_results};
use crate::protocol_admin_share_group::{deadline_after, public_description, retry_safe};
use crate::state::AdapterState;

pub(crate) fn describe<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DescribeShareGroupsCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .describe_share_groups(command.group_ids.clone())
                .include_authorized_operations(command.include_authorized_operations)
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let groups = ordered_group_results(
        result.into_groups().into_entries(),
        &command.group_ids,
        &command.operation_id,
        "Share-group description",
    )?;
    let outcomes = groups
        .into_iter()
        .map(|group| match group.result {
            ResourceResult::Success(value) => {
                let description = public_description(command.operation_id.clone(), value);
                if description.group_id != group.group_id {
                    return Err(AdapterError::AdminResult(format!(
                        "admin operation {} returned mismatched inner Share-group identity",
                        command.operation_id
                    )));
                }
                Ok(AdminShareGroupDescriptionOutcome {
                    group_id: group.group_id,
                    description: Some(description),
                    error_code: None,
                })
            }
            ResourceResult::Failure(error_code) => Ok(AdminShareGroupDescriptionOutcome {
                group_id: group.group_id,
                description: None,
                error_code: Some(error_code),
            }),
        })
        .collect::<Result<Vec<_>, AdapterError>>()?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ShareGroupsDescribed(AdminShareGroupsDescription {
                operation_id: command.operation_id,
                outcomes,
            }),
        ),
    )
}
