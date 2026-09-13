//! Client-quota administration maps one bounded named-user byte-rate operation.

use std::io::Write;
use std::time::Duration;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, AdminClientQuotaAlteration,
    AdminClientQuotaDescription, BrokerQuotaDirection, CommandId, OperationId,
};

use crate::AdapterError;
use crate::kafkars_api::{
    ClientQuotaAlteration, ClientQuotaAlterationOperation, ClientQuotaEntity,
    ClientQuotaEntityComponent, ClientQuotaFilterComponent,
};
use crate::protocol::emit;
use crate::protocol_admin_result::take_single_result;
use crate::state::AdapterState;

pub(crate) fn dispatch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    match command {
        AdapterCommand::AlterClientQuota(command) => alter(state, writer, command_id, command),
        AdapterCommand::DescribeClientQuota(command) => {
            describe(state, writer, command_id, command)
        }
        _ => Err(AdapterError::AdminResult(
            "non-client-quota command reached client-quota dispatcher".to_owned(),
        )),
    }
}

fn alter<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: testlab_schema::AlterClientQuotaCommand,
) -> Result<(), AdapterError> {
    let validate_only = command.validate_only;
    let entity = public_entity(&command.user);
    let operation = match command.bytes_per_second {
        Some(value) => ClientQuotaAlterationOperation::set(
            command.direction.config_name(),
            public_rate(value, &command.operation_id)?,
        ),
        None => ClientQuotaAlterationOperation::remove(command.direction.config_name()),
    };
    let result = state
        .client(&command.client_id)?
        .admin()
        .alter_client_quotas([ClientQuotaAlteration::new(entity.clone(), [operation])])
        .validate_only(validate_only)
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    take_single_result(
        result.into_entities().into_entries(),
        &command.operation_id,
        |actual| actual == &entity,
        "client-quota entity",
    )?;
    let completion = AdminClientQuotaAlteration {
        operation_id: command.operation_id,
        user: command.user,
        direction: command.direction,
    };
    let event = if validate_only {
        AdapterEvent::ClientQuotaAlterationValidated(completion)
    } else {
        AdapterEvent::ClientQuotaAltered(completion)
    };
    emit_quota(writer, command_id, event)
}

fn describe<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: testlab_schema::DescribeClientQuotaCommand,
) -> Result<(), AdapterError> {
    let result = state
        .client(&command.client_id)?
        .admin()
        .describe_client_quotas([ClientQuotaFilterComponent::exact(
            "user",
            command.user.clone(),
        )])
        .strict(true)
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let rate = described_rate(
        result.into_entries(),
        &command.operation_id,
        &command.user,
        command.direction,
    )?;
    emit_quota(
        writer,
        command_id,
        AdapterEvent::ClientQuotaDescribed(AdminClientQuotaDescription {
            operation_id: command.operation_id,
            user: command.user,
            direction: command.direction,
            bytes_per_second: rate,
        }),
    )
}

pub(crate) fn described_rate(
    entries: Vec<crate::kafkars_api::ClientQuotaEntry>,
    operation_id: &OperationId,
    expected_user: &str,
    direction: BrokerQuotaDirection,
) -> Result<u64, AdapterError> {
    let mut entries = entries.into_iter();
    let Some(entry) = entries.next() else {
        return Err(invalid(operation_id, "returned no client-quota entity"));
    };
    if entries.next().is_some() {
        return Err(invalid(
            operation_id,
            "returned multiple client-quota entities",
        ));
    }
    let (components, values) = entry.into_parts();
    let mut components = components.into_iter();
    let Some(component) = components.next() else {
        return Err(invalid(operation_id, "returned no quota entity component"));
    };
    if components.next().is_some()
        || component.entity_type() != "user"
        || component.entity_name() != Some(expected_user)
    {
        return Err(invalid(
            operation_id,
            "returned an unexpected quota entity component",
        ));
    }
    let mut values = values.into_iter();
    let Some(value) = values.next() else {
        return Err(invalid(operation_id, "returned no selected quota value"));
    };
    if values.next().is_some() || value.key() != direction.config_name() {
        return Err(invalid(
            operation_id,
            "returned an unexpected selected quota value",
        ));
    }
    whole_rate(value.value(), operation_id)
}

pub(crate) fn public_entity(user: &str) -> ClientQuotaEntity {
    ClientQuotaEntity::new([ClientQuotaEntityComponent::named("user", user)])
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::float_cmp,
    reason = "Testlab intentionally accepts only exactly integral public quota values"
)]
pub(crate) fn whole_rate(value: f64, operation_id: &OperationId) -> Result<u64, AdapterError> {
    if !value.is_finite() || !(1.0..=f64::from(u32::MAX)).contains(&value) || value.fract() != 0.0 {
        return Err(invalid(
            operation_id,
            "returned a non-integral or out-of-range quota value",
        ));
    }
    Ok(value as u64)
}

fn public_rate(value: u64, operation_id: &OperationId) -> Result<f64, AdapterError> {
    let value = u32::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| invalid(operation_id, "received an out-of-range quota value"))?;
    Ok(f64::from(value))
}

fn emit_quota<W: Write>(
    writer: &mut W,
    command_id: CommandId,
    event: AdapterEvent,
) -> Result<(), AdapterError> {
    emit(writer, &AdapterEventEnvelope::new(command_id, event))
}

fn invalid(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
