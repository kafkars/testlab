//! Delegation-token qualification retains public facts while secrets stay process-local.

use std::io::Write;
use std::time::{Duration, Instant};

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminDelegationTokenLifecycle, CommandId,
    DelegationTokenPrincipalSpec, ExerciseDelegationTokenLifecycleCommand,
};

use crate::AdapterError;
use crate::kafkars_api::{
    DelegationToken, DelegationTokenHmac, DelegationTokenPrincipal, ErrorKind, KafkaError,
};
use crate::protocol::emit;
use crate::state::AdapterState;

#[derive(Debug, Eq, PartialEq)]
struct TokenSnapshot {
    owner: DelegationTokenPrincipalSpec,
    requester: Option<DelegationTokenPrincipalSpec>,
    renewers: Vec<DelegationTokenPrincipalSpec>,
    token_id: String,
    issue_timestamp_ms: i64,
    expiry_timestamp_ms: i64,
    max_timestamp_ms: i64,
}

pub(crate) fn exercise<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: ExerciseDelegationTokenLifecycleCommand,
) -> Result<(), AdapterError> {
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(command.timeout_ms))
        .ok_or_else(|| invalid(&command, "deadline overflowed"))?;
    let client = state.client(&command.client_id)?;
    let admin = client.admin();
    let owner = principal(&command.owner);
    let renewers = command.renewers.iter().map(principal).collect::<Vec<_>>();
    let created_result = admin
        .create_delegation_token()
        .owner(owner.clone())
        .renewers(renewers)
        .max_lifetime(Duration::from_millis(command.max_lifetime_ms))
        .deadline_after(remaining(deadline)?)
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let create_throttle_time_ms = throttle(created_result.throttle_time(), &command, "create")?;
    let created = created_result.into_token();
    let created_snapshot = snapshot(&created);
    let hmac_size = created.hmac().len();

    let described_result = admin
        .describe_delegation_tokens()
        .owners([owner])
        .deadline_after(remaining(deadline)?)
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let describe_throttle_time_ms =
        throttle(described_result.throttle_time(), &command, "describe")?;
    let mut described = described_result.into_tokens();
    if described.len() != 1 {
        return Err(invalid(
            &command,
            "owner-filtered description did not return exactly one token",
        ));
    }
    let described = described
        .pop()
        .ok_or_else(|| invalid(&command, "owner-filtered token disappeared"))?;
    let description_matched = snapshot(&described) == created_snapshot
        && described.hmac().as_bytes() == created.hmac().as_bytes();

    let renewed_result = admin
        .renew_delegation_token(secret_copy(&created, &command)?)
        .renew_for(Duration::from_millis(command.renew_period_ms))
        .deadline_after(remaining(deadline)?)
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let renew_throttle_time_ms = throttle(renewed_result.throttle_time(), &command, "renew")?;
    let renewed_expiry_timestamp_ms = renewed_result.expiry_timestamp_ms();

    let expiration = admin.expire_delegation_token(secret_copy(&created, &command)?);
    let expiration = match command.expire_after_ms {
        Some(period) => expiration.expire_after(Duration::from_millis(period)),
        None => expiration,
    };
    let expired_result = expiration
        .deadline_after(remaining(deadline)?)
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let expire_throttle_time_ms = throttle(expired_result.throttle_time(), &command, "expire")?;
    let expired_at_timestamp_ms = expired_result.expiry_timestamp_ms();

    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::DelegationTokenLifecycleExercised(AdminDelegationTokenLifecycle {
                operation_id: command.operation_id,
                owner: created_snapshot.owner,
                requester: created_snapshot.requester,
                renewers: created_snapshot.renewers,
                token_id: created_snapshot.token_id,
                issue_timestamp_ms: created_snapshot.issue_timestamp_ms,
                initial_expiry_timestamp_ms: created_snapshot.expiry_timestamp_ms,
                max_timestamp_ms: created_snapshot.max_timestamp_ms,
                renewed_expiry_timestamp_ms,
                expired_at_timestamp_ms,
                hmac_size,
                description_matched,
                create_throttle_time_ms,
                describe_throttle_time_ms,
                renew_throttle_time_ms,
                expire_throttle_time_ms,
            }),
        ),
    )
}

fn snapshot(token: &DelegationToken) -> TokenSnapshot {
    TokenSnapshot {
        owner: snapshot_principal(token.owner()),
        requester: token.requester().map(snapshot_principal),
        renewers: token.renewers().iter().map(snapshot_principal).collect(),
        token_id: token.token_id().to_owned(),
        issue_timestamp_ms: token.issue_timestamp_ms(),
        expiry_timestamp_ms: token.expiry_timestamp_ms(),
        max_timestamp_ms: token.max_timestamp_ms(),
    }
}

fn principal(value: &DelegationTokenPrincipalSpec) -> DelegationTokenPrincipal {
    DelegationTokenPrincipal::new(value.principal_type.clone(), value.principal_name.clone())
}

fn snapshot_principal(value: &DelegationTokenPrincipal) -> DelegationTokenPrincipalSpec {
    DelegationTokenPrincipalSpec {
        principal_type: value.principal_type().to_owned(),
        principal_name: value.principal_name().to_owned(),
    }
}

fn secret_copy(
    token: &DelegationToken,
    command: &ExerciseDelegationTokenLifecycleCommand,
) -> Result<DelegationTokenHmac, AdapterError> {
    DelegationTokenHmac::from_bytes(token.hmac().as_bytes().to_vec())
        .map_err(|_| invalid(command, "could not reconstruct a bounded token secret"))
}

fn throttle(
    value: Duration,
    command: &ExerciseDelegationTokenLifecycleCommand,
    operation: &str,
) -> Result<u64, AdapterError> {
    u64::try_from(value.as_millis()).map_err(|_| {
        invalid(
            command,
            &format!("{operation} returned an unrepresentable throttle time"),
        )
    })
}

fn remaining(deadline: Instant) -> Result<Duration, AdapterError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(AdapterError::Client(KafkaError::new(
            ErrorKind::Timeout,
            "delegation-token lifecycle deadline elapsed",
        )))
    } else {
        Ok(remaining)
    }
}

fn invalid(command: &ExerciseDelegationTokenLifecycleCommand, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {} {detail}", command.operation_id))
}
