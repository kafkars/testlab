//! Classic-group description uses the dedicated packaged public operation.

use std::io::Write;
use std::time::Duration;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminClassicGroupDescriptionOutcome,
    AdminClassicGroupsDescription, CommandId, DescribeClassicGroupsCommand,
};

use crate::AdapterError;
use crate::protocol::emit;
use crate::protocol_admin_plural_result::{GroupResult, ResourceResult, ordered_group_results};
use crate::state::AdapterState;

pub(crate) fn describe<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DescribeClassicGroupsCommand,
) -> Result<(), AdapterError> {
    let result = state
        .client(&command.client_id)?
        .admin()
        .describe_classic_groups(command.group_ids.clone())
        .include_authorized_operations(false)
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let entries = result
        .into_groups()
        .into_entries()
        .into_iter()
        .map(|(group_id, result)| (group_id, result.map(|group| group.members().len())))
        .collect();
    let groups = ordered_group_results(
        entries,
        &command.group_ids,
        &command.operation_id,
        "classic-group description",
    )?;
    let outcomes = description_outcomes(groups, &command.operation_id)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ClassicGroupsDescribed(AdminClassicGroupsDescription {
                operation_id: command.operation_id,
                outcomes,
            }),
        ),
    )
}

pub(crate) fn description_outcomes(
    groups: Vec<GroupResult<usize>>,
    operation_id: &testlab_schema::OperationId,
) -> Result<Vec<AdminClassicGroupDescriptionOutcome>, AdapterError> {
    groups
        .into_iter()
        .map(|group| match group.result {
            ResourceResult::Success(member_count) => {
                let member_count = u32::try_from(member_count).map_err(|_| {
                    AdapterError::AdminResult(format!(
                        "admin operation {operation_id} returned too many classic-group members"
                    ))
                })?;
                Ok(AdminClassicGroupDescriptionOutcome {
                    group_id: group.group_id,
                    member_count: Some(member_count),
                    error_code: None,
                })
            }
            ResourceResult::Failure(error_code) => Ok(AdminClassicGroupDescriptionOutcome {
                group_id: group.group_id,
                member_count: None,
                error_code: Some(error_code),
            }),
        })
        .collect()
}
