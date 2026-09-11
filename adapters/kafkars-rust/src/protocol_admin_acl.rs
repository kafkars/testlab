//! ACL administration maps the bounded Testlab model to Kafkars' public ACL surface.

use std::io::Write;
use std::time::{Duration, Instant};

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, AdminAclsCreation, AdminAclsDeletion,
    AdminAclsDescription, CommandId,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::kafkars_api::{AclBindingFilter, KafkaError, RetryAdvice};
use crate::protocol::emit;
use crate::protocol_admin_acl_mapping::{public_binding, schema_binding};
use crate::protocol_admin_acl_result::{creation_outcomes, deletion_outcomes};
use crate::state::AdapterState;

pub(crate) fn dispatch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    match command {
        AdapterCommand::CreateAcls(command) => create(state, writer, command_id, command),
        AdapterCommand::DescribeAcls(command) => describe(state, writer, command_id, command),
        AdapterCommand::DeleteAcls(command) => delete(state, writer, command_id, command),
        _ => Err(AdapterError::AdminResult(
            "non-ACL command reached ACL dispatcher".to_owned(),
        )),
    }
}

fn create<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: testlab_schema::CreateAclsCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .create_acls(command.bindings.iter().map(public_binding))
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let outcomes = creation_outcomes(
        result.into_outcomes(),
        &command.bindings,
        &command.operation_id,
    )?;
    emit_acl(
        writer,
        command_id,
        AdapterEvent::AclsCreated(AdminAclsCreation {
            operation_id: command.operation_id,
            outcomes,
        }),
    )
}

fn describe<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: testlab_schema::DescribeAclsCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let filter = AclBindingFilter::from_binding(&public_binding(&command.binding));
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .describe_acls(filter.clone())
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let bindings = result
        .into_bindings()
        .into_iter()
        .map(|binding| schema_binding(&binding, &command.operation_id))
        .collect::<Result<Vec<_>, _>>()?;
    emit_acl(
        writer,
        command_id,
        AdapterEvent::AclsDescribed(AdminAclsDescription {
            operation_id: command.operation_id,
            bindings,
        }),
    )
}

fn delete<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: testlab_schema::DeleteAclsCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            let filters = command
                .bindings
                .iter()
                .map(|binding| AclBindingFilter::from_binding(&public_binding(binding)));
            client
                .admin()
                .delete_acls(filters)
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let outcomes = deletion_outcomes(
        result.into_outcomes(),
        &command.bindings,
        &command.operation_id,
    )?;
    emit_acl(
        writer,
        command_id,
        AdapterEvent::AclsDeleted(AdminAclsDeletion {
            operation_id: command.operation_id,
            outcomes,
        }),
    )
}

fn deadline_after(timeout_ms: u64) -> Instant {
    let started = Instant::now();
    started
        .checked_add(Duration::from_millis(timeout_ms))
        .unwrap_or(started)
}

fn retry_safe(error: &KafkaError) -> bool {
    error.retry_advice() == RetryAdvice::RetrySafe
}

fn emit_acl<W: Write>(
    writer: &mut W,
    command_id: CommandId,
    event: AdapterEvent,
) -> Result<(), AdapterError> {
    emit(writer, &AdapterEventEnvelope::new(command_id, event))
}
