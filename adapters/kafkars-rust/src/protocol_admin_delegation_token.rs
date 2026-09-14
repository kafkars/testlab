//! Delegation-token qualification retains public facts while secrets stay process-local.

use std::io::Write;
use std::thread;
use std::time::{Duration, Instant};

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminDelegationTokenLifecycle, CommandId,
    DelegationTokenPrincipalSpec, ExerciseDelegationTokenLifecycleCommand,
};

use crate::AdapterError;
use crate::kafkars_api::{
    DelegationToken, DelegationTokenHmac, DelegationTokenPrincipal, ErrorKind, KafkaError,
    RetryAdvice,
};
use crate::protocol::emit;
use crate::state::AdapterState;

const DESCRIPTION_POLL_INTERVAL: Duration = Duration::from_millis(10);
const RENEW_POLL_INTERVAL: Duration = Duration::from_millis(10);
const TOKEN_NOT_FOUND_RETRY_WINDOW: Duration = Duration::from_secs(1);
const TOKEN_NOT_FOUND: i16 = 62;

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

    let (describe_throttle_time, mut described) = loop {
        let described_result = admin
            .describe_delegation_tokens()
            .owners([owner.clone()])
            .deadline_after(remaining(deadline)?)
            .submit()
            .wait()
            .map_err(AdapterError::Client)?;
        let (throttle_time, tokens) = described_result.into_parts();
        match description_decision(tokens.len(), Instant::now() >= deadline) {
            Ok(DescriptionDecision::Accept) => break (throttle_time, tokens),
            Ok(DescriptionDecision::Retry) => {
                thread::sleep(DESCRIPTION_POLL_INTERVAL.min(remaining(deadline)?));
            }
            Err(detail) => return Err(invalid(&command, detail)),
        }
    };
    let describe_throttle_time_ms = throttle(describe_throttle_time, &command, "describe")?;
    let described = described
        .pop()
        .ok_or_else(|| invalid(&command, "owner-filtered token disappeared"))?;
    let description_matched = snapshot(&described) == created_snapshot
        && described.hmac().as_bytes() == created.hmac().as_bytes();

    let (renew_throttle_time, renewed_expiry_timestamp_ms) =
        renew_fresh_token(&admin, &created, &command, deadline)?;
    let renew_throttle_time_ms = throttle(renew_throttle_time, &command, "renew")?;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescriptionDecision {
    Retry,
    Accept,
}

pub(crate) const fn description_decision(
    token_count: usize,
    deadline_elapsed: bool,
) -> Result<DescriptionDecision, &'static str> {
    match (token_count, deadline_elapsed) {
        (0, false) => Ok(DescriptionDecision::Retry),
        (1, false) => Ok(DescriptionDecision::Accept),
        (_, true) => Err("owner-filtered description exceeded the lifecycle deadline"),
        _ => Err("owner-filtered description did not return exactly one token"),
    }
}

pub(crate) fn renew_retryable(
    error: &KafkaError,
    now: Instant,
    token_not_found_deadline: Instant,
    lifecycle_deadline: Instant,
) -> bool {
    renew_retryable_parts(
        error.retry_advice(),
        error.broker_code(),
        now < token_not_found_deadline,
        now >= lifecycle_deadline,
    )
}

pub(crate) fn renew_retryable_parts(
    advice: RetryAdvice,
    broker_code: Option<i16>,
    token_not_found_window_open: bool,
    deadline_elapsed: bool,
) -> bool {
    !deadline_elapsed
        && (advice == RetryAdvice::RetrySafe
            || (broker_code == Some(TOKEN_NOT_FOUND) && token_not_found_window_open))
}

fn renew_fresh_token(
    admin: &crate::kafkars_api::Admin,
    created: &DelegationToken,
    command: &ExerciseDelegationTokenLifecycleCommand,
    deadline: Instant,
) -> Result<(Duration, i64), AdapterError> {
    let token_not_found_deadline = Instant::now()
        .checked_add(TOKEN_NOT_FOUND_RETRY_WINDOW)
        .unwrap_or(deadline)
        .min(deadline);
    loop {
        let result = admin
            .renew_delegation_token(secret_copy(created, command)?)
            .renew_for(Duration::from_millis(command.renew_period_ms))
            .deadline_after(remaining(deadline)?)
            .submit()
            .wait();
        let now = Instant::now();
        match result {
            Ok(result) => return Ok((result.throttle_time(), result.expiry_timestamp_ms())),
            Err(error) if renew_retryable(&error, now, token_not_found_deadline, deadline) => {
                thread::sleep(RENEW_POLL_INTERVAL.min(remaining(deadline)?));
            }
            Err(error) => return Err(AdapterError::Client(error)),
        }
    }
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
