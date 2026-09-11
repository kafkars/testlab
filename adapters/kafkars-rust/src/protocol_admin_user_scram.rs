//! User SCRAM administration maps one bounded named-user credential operation.

use std::io::Write;
use std::time::Duration;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, AdminUserScramCredentialAlteration,
    AdminUserScramCredentialDescription, CommandId, OperationId, SASL_PASSWORD_ENVIRONMENT,
    ScramCredentialMechanism,
};

use crate::AdapterError;
use crate::kafkars_api::{
    ErrorKind, KafkaError, ScramCredentialInfo, ScramMechanism, UserScramCredentialAlteration,
};
use crate::protocol::emit;
use crate::protocol_admin_result::take_single_result;
use crate::state::AdapterState;

const RESOURCE_NOT_FOUND: i16 = 91;

pub(crate) fn dispatch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    match command {
        AdapterCommand::AlterUserScramCredential(command) => {
            alter(state, writer, command_id, command)
        }
        AdapterCommand::DescribeUserScramCredential(command) => {
            describe(state, writer, command_id, command)
        }
        _ => Err(AdapterError::AdminResult(
            "non-user-SCRAM command reached user-SCRAM dispatcher".to_owned(),
        )),
    }
}

fn alter<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: testlab_schema::AlterUserScramCredentialCommand,
) -> Result<(), AdapterError> {
    let mechanism = public_mechanism(command.mechanism);
    let alteration = match command.iterations {
        Some(iterations) => UserScramCredentialAlteration::upsert(
            command.user.clone(),
            mechanism,
            iterations,
            password_bytes()?,
        ),
        None => UserScramCredentialAlteration::delete(command.user.clone(), mechanism),
    };
    let result = state
        .client(&command.client_id)?
        .admin()
        .alter_user_scram_credentials([alteration])
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    take_single_result(
        result.into_users().into_entries(),
        &command.operation_id,
        |actual| actual == &command.user,
        "user SCRAM alteration",
    )?;
    emit_scram(
        writer,
        command_id,
        AdapterEvent::UserScramCredentialAltered(AdminUserScramCredentialAlteration {
            operation_id: command.operation_id,
            user: command.user,
            mechanism: command.mechanism,
        }),
    )
}

fn describe<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: testlab_schema::DescribeUserScramCredentialCommand,
) -> Result<(), AdapterError> {
    let result = state
        .client(&command.client_id)?
        .admin()
        .describe_user_scram_credentials()
        .users([command.user.clone()])
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let (iterations, absence_broker_code) = described_credential(
        result.into_users().into_entries(),
        &command.operation_id,
        &command.user,
        command.mechanism,
    )?;
    emit_scram(
        writer,
        command_id,
        AdapterEvent::UserScramCredentialDescribed(AdminUserScramCredentialDescription {
            operation_id: command.operation_id,
            user: command.user,
            mechanism: command.mechanism,
            iterations,
            absence_broker_code,
        }),
    )
}

pub(crate) fn described_credential(
    entries: Vec<(String, Result<Vec<ScramCredentialInfo>, KafkaError>)>,
    operation_id: &OperationId,
    expected_user: &str,
    mechanism: ScramCredentialMechanism,
) -> Result<(Option<u32>, Option<i16>), AdapterError> {
    let mut entries = entries.into_iter();
    let Some((user, result)) = entries.next() else {
        return Err(invalid(operation_id, "returned no user result"));
    };
    if entries.next().is_some() || user != expected_user {
        return Err(invalid(operation_id, "returned an unexpected user result"));
    }
    match result {
        Ok(credentials) => {
            let [credential] = credentials.as_slice() else {
                return Err(invalid(
                    operation_id,
                    "returned an unexpected credential set",
                ));
            };
            if credential.mechanism() != public_mechanism(mechanism) {
                return Err(invalid(
                    operation_id,
                    "returned an unexpected credential mechanism",
                ));
            }
            Ok((Some(credential.iterations()), None))
        }
        Err(error)
            if error.kind() == ErrorKind::Broker
                && error.broker_code() == Some(RESOURCE_NOT_FOUND) =>
        {
            Ok((None, Some(RESOURCE_NOT_FOUND)))
        }
        Err(error) => Err(AdapterError::Client(error)),
    }
}

pub(crate) const fn public_mechanism(value: ScramCredentialMechanism) -> ScramMechanism {
    match value {
        ScramCredentialMechanism::Sha256 => ScramMechanism::SHA_256,
        ScramCredentialMechanism::Sha512 => ScramMechanism::SHA_512,
    }
}

fn password_bytes() -> Result<Vec<u8>, AdapterError> {
    std::env::var(SASL_PASSWORD_ENVIRONMENT)
        .map(String::into_bytes)
        .map_err(|_| {
            AdapterError::State(format!(
                "required security environment variable {SASL_PASSWORD_ENVIRONMENT} is unavailable"
            ))
        })
}

fn emit_scram<W: Write>(
    writer: &mut W,
    command_id: CommandId,
    event: AdapterEvent,
) -> Result<(), AdapterError> {
    emit(writer, &AdapterEventEnvelope::new(command_id, event))
}

fn invalid(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
