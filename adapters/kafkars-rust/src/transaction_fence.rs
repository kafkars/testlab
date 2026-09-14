//! Transaction fencing interleaves public owners while retaining both handles for cleanup.

use std::io::Write;
use std::thread;
use std::time::{Duration, Instant};

use crate::kafkars_api::{ErrorKind, KafkaError, RetryAdvice, Transaction, TransactionalProducer};
use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, ClientId, CommandId, ProducerId,
    TransactionFenceMethod,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::normalize;
use crate::protocol::emit;
use crate::state::AdapterState;
use crate::transaction_execute;

pub(crate) fn dispatch<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    let AdapterCommand::FenceTransaction {
        fence_method,
        producer_id,
        transaction_id,
        operation,
        replacement_client_id,
        replacement_producer_id,
        transactional_id,
        transaction_timeout_ms,
        initialization_timeout_ms,
        timeout_ms,
    } = command
    else {
        return Err(AdapterError::TransactionResult(
            "non-fence command reached transaction fence dispatcher".to_owned(),
        ));
    };
    let mut owner = state.take_transactional_producer(&producer_id)?;
    let result = execute(
        &mut owner.producer,
        state,
        writer,
        command_id,
        fence_method,
        transaction_id,
        operation,
        &replacement_client_id,
        &replacement_producer_id,
        &transactional_id,
        Duration::from_millis(transaction_timeout_ms),
        Duration::from_millis(initialization_timeout_ms),
        Duration::from_millis(timeout_ms),
    );
    state.restore_transactional_producer(producer_id, owner)?;
    result
}

#[allow(
    clippy::too_many_arguments,
    reason = "the external command keeps every public fencing input explicit"
)]
fn execute<W: Write>(
    producer: &mut TransactionalProducer,
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    fence_method: TransactionFenceMethod,
    transaction_id: testlab_schema::OperationId,
    operation: testlab_schema::BatchRecord,
    replacement_client_id: &testlab_schema::ClientId,
    replacement_producer_id: &ProducerId,
    transactional_id: &str,
    transaction_timeout: Duration,
    initialization_timeout: Duration,
    timeout: Duration,
) -> Result<(), AdapterError> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| timeout_error("transaction fence deadline overflow"))?;
    loop {
        match producer.begin() {
            Ok(transaction) => {
                return execute_started(
                    transaction,
                    state,
                    writer,
                    command_id,
                    fence_method,
                    transaction_id,
                    operation,
                    replacement_client_id,
                    replacement_producer_id,
                    transactional_id,
                    transaction_timeout,
                    initialization_timeout,
                    deadline,
                );
            }
            Err(error)
                if error.retry_advice() == RetryAdvice::RetrySafe && Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(AdapterError::Client(error)),
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the active transaction retains every explicit fencing input"
)]
fn execute_started<W: Write>(
    mut transaction: Transaction<'_>,
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    fence_method: TransactionFenceMethod,
    transaction_id: testlab_schema::OperationId,
    operation: testlab_schema::BatchRecord,
    replacement_client_id: &testlab_schema::ClientId,
    replacement_producer_id: &ProducerId,
    transactional_id: &str,
    transaction_timeout: Duration,
    initialization_timeout: Duration,
    deadline: Instant,
) -> Result<(), AdapterError> {
    transaction_execute::send(
        &mut transaction,
        writer,
        &command_id,
        operation,
        None,
        deadline,
    )?;
    let commit_error_code = match fence_method {
        TransactionFenceMethod::ReplacementInitialization => {
            create_replacement(
                state,
                writer,
                &command_id,
                replacement_client_id,
                replacement_producer_id,
                transactional_id,
                transaction_timeout,
                initialization_timeout,
                deadline,
            )?;
            commit_result(transaction, deadline)?
        }
        TransactionFenceMethod::AdminForceTermination => {
            let client = state.client(replacement_client_id)?;
            retry_until_with_remaining(
                deadline,
                |remaining| {
                    client
                        .admin()
                        .force_terminate_transaction(transactional_id)
                        .deadline_after(remaining)
                        .submit()
                        .wait()
                },
                retry_force_termination,
            )
            .map_err(AdapterError::Client)?;
            let result = commit_result(transaction, deadline)?;
            create_replacement(
                state,
                writer,
                &command_id,
                replacement_client_id,
                replacement_producer_id,
                transactional_id,
                transaction_timeout,
                initialization_timeout,
                deadline,
            )?;
            result
        }
    };
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::TransactionFenceCompleted {
                transaction_id,
                commit_error_code,
            },
        ),
    )
}

fn retry_force_termination(error: &KafkaError) -> bool {
    retry_force_termination_parts(error.retry_advice(), error.broker_code())
}

const fn retry_force_termination_parts(advice: RetryAdvice, broker_code: Option<i16>) -> bool {
    matches!(advice, RetryAdvice::RetrySafe) || matches!(broker_code, Some(51))
}

#[cfg(test)]
#[path = "transaction_fence_test.rs"]
mod tests;

#[allow(
    clippy::too_many_arguments,
    reason = "replacement initialization retains the command's exact public inputs"
)]
fn create_replacement<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: &CommandId,
    client_id: &ClientId,
    producer_id: &ProducerId,
    transactional_id: &str,
    transaction_timeout: Duration,
    initialization_timeout: Duration,
    deadline: Instant,
) -> Result<(), AdapterError> {
    let observation = state.create_transactional_producer(
        client_id.clone(),
        producer_id.clone(),
        transactional_id,
        transaction_timeout,
        initialization_timeout.min(remaining(deadline)?),
    )?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id.clone(),
            AdapterEvent::TransactionalProducerCreated(observation),
        ),
    )
}

fn commit_result(
    mut transaction: Transaction<'_>,
    deadline: Instant,
) -> Result<Option<String>, AdapterError> {
    loop {
        match transaction.commit(remaining(deadline)?) {
            Ok(observer) => {
                return Ok(observer
                    .wait()
                    .err()
                    .map(|error| normalize::error_code(&error)));
            }
            Err(rejection) => {
                let (returned, error) = rejection.into_parts();
                transaction = returned;
                if error.retry_advice() != RetryAdvice::RetrySafe || Instant::now() >= deadline {
                    return Ok(Some(normalize::error_code(&error)));
                }
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

fn remaining(deadline: Instant) -> Result<Duration, AdapterError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(timeout_error("transaction fence deadline elapsed"))
    } else {
        Ok(remaining)
    }
}

fn timeout_error(message: &str) -> AdapterError {
    AdapterError::Client(KafkaError::new(ErrorKind::Timeout, message))
}
