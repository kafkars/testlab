//! Transaction fencing interleaves public owners while retaining both handles for cleanup.

use std::io::Write;
use std::thread;
use std::time::{Duration, Instant};

use crate::kafkars_api::{ErrorKind, KafkaError, RetryAdvice, Transaction, TransactionalProducer};
use testlab_schema::{AdapterCommand, AdapterEvent, AdapterEventEnvelope, CommandId, ProducerId};

use crate::AdapterError;
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
        transaction_id,
        operation,
        replacement_client_id,
        replacement_producer_id,
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
    transaction_id: testlab_schema::OperationId,
    operation: testlab_schema::BatchRecord,
    replacement_client_id: testlab_schema::ClientId,
    replacement_producer_id: ProducerId,
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
    transaction_id: testlab_schema::OperationId,
    operation: testlab_schema::BatchRecord,
    replacement_client_id: testlab_schema::ClientId,
    replacement_producer_id: ProducerId,
    transactional_id: &str,
    transaction_timeout: Duration,
    initialization_timeout: Duration,
    deadline: Instant,
) -> Result<(), AdapterError> {
    transaction_execute::send(&mut transaction, writer, &command_id, operation, deadline)?;
    state.create_transactional_producer(
        replacement_client_id,
        replacement_producer_id.clone(),
        transactional_id,
        transaction_timeout,
        initialization_timeout.min(remaining(deadline)?),
    )?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id.clone(),
            AdapterEvent::TransactionalProducerCreated {
                producer_id: replacement_producer_id,
            },
        ),
    )?;
    let commit_error_code = commit_result(transaction, deadline)?;
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
