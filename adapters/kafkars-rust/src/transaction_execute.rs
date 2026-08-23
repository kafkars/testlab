//! Transaction commands preserve public admission, staging, and end disposition.

use std::io::Write;
use std::thread;
use std::time::{Duration, Instant};

use kafkars::{ErrorKind, RetryAdvice, Transaction, TransactionalProducer};
use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, BatchRecord, CommandId, OperationId,
    TerminalStatus, TransactionDisposition,
};

use crate::AdapterError;
use crate::normalize;
use crate::protocol::emit;
use crate::state::AdapterState;

pub(crate) fn dispatch<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    match command {
        AdapterCommand::CreateTransactionalProducer {
            client_id,
            producer_id,
            transactional_id,
            transaction_timeout_ms,
            initialization_timeout_ms,
        } => {
            state.create_transactional_producer(
                client_id,
                producer_id.clone(),
                &transactional_id,
                Duration::from_millis(transaction_timeout_ms),
                Duration::from_millis(initialization_timeout_ms),
            )?;
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::TransactionalProducerCreated { producer_id },
                ),
            )
        }
        AdapterCommand::ExecuteTransaction {
            producer_id,
            transaction_id,
            operations,
            disposition,
            timeout_ms,
        } => execute(
            state.transactional_producer_mut(&producer_id)?,
            writer,
            command_id,
            transaction_id,
            operations,
            disposition,
            Duration::from_millis(timeout_ms),
        ),
        AdapterCommand::CloseTransactionalProducer { producer_id } => {
            state.close_transactional_producer(&producer_id)?;
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::TransactionalProducerClosed { producer_id },
                ),
            )
        }
        _ => Err(AdapterError::TransactionResult(
            "non-transaction command reached transaction dispatcher".to_owned(),
        )),
    }
}

fn execute<W: Write>(
    producer: &mut TransactionalProducer,
    writer: &mut W,
    command_id: CommandId,
    transaction_id: OperationId,
    operations: Vec<BatchRecord>,
    disposition: TransactionDisposition,
    timeout: Duration,
) -> Result<(), AdapterError> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| timeout_error("transaction deadline overflow"))?;
    loop {
        match producer.begin() {
            Ok(transaction) => {
                return execute_started(
                    transaction,
                    writer,
                    command_id,
                    transaction_id,
                    operations,
                    disposition,
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

fn execute_started<W: Write>(
    mut transaction: Transaction<'_>,
    writer: &mut W,
    command_id: CommandId,
    transaction_id: OperationId,
    operations: Vec<BatchRecord>,
    disposition: TransactionDisposition,
    deadline: Instant,
) -> Result<(), AdapterError> {
    for operation in operations {
        send(&mut transaction, writer, &command_id, operation, deadline)?;
    }
    end(transaction, disposition, deadline)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::TransactionCompleted {
                transaction_id,
                disposition,
            },
        ),
    )
}

pub(crate) fn send<W: Write>(
    transaction: &mut Transaction<'_>,
    writer: &mut W,
    command_id: &CommandId,
    operation: BatchRecord,
    deadline: Instant,
) -> Result<(), AdapterError> {
    let operation_id = operation.operation_id;
    let mut record = normalize::record(operation.record)?;
    let observer = loop {
        match transaction.send(record, remaining(deadline)?) {
            Ok(observer) => break observer,
            Err(rejection) => {
                let (returned, error) = rejection.into_parts();
                if error.retry_advice() == RetryAdvice::RetrySafe && Instant::now() < deadline {
                    record = returned;
                    thread::sleep(Duration::from_millis(1));
                    continue;
                }
                emit(
                    writer,
                    &AdapterEventEnvelope::new(
                        command_id.clone(),
                        AdapterEvent::OperationRejected {
                            operation_id,
                            code: normalize::error_code(&error),
                        },
                    ),
                )?;
                return Err(AdapterError::Client(error));
            }
        }
    };
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id.clone(),
            AdapterEvent::OperationAccepted {
                operation_id: operation_id.clone(),
            },
        ),
    )?;
    let (status, code, offset) = match observer.wait() {
        Ok(metadata) => (
            TerminalStatus::TransactionStaged,
            None,
            Some(metadata.offset()),
        ),
        Err(error) => {
            let failure = normalize::delivery_failure(&error);
            (failure.status, Some(failure.code), None)
        }
    };
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id.clone(),
            AdapterEvent::OperationTerminal {
                operation_id,
                status,
                code,
                offset,
            },
        ),
    )
}

fn end(
    transaction: Transaction<'_>,
    disposition: TransactionDisposition,
    deadline: Instant,
) -> Result<(), AdapterError> {
    match disposition {
        TransactionDisposition::Commit => commit(transaction, deadline),
        TransactionDisposition::Abort => abort(transaction, deadline),
    }
}

fn commit(mut transaction: Transaction<'_>, deadline: Instant) -> Result<(), AdapterError> {
    loop {
        match transaction.commit(remaining(deadline)?) {
            Ok(observer) => return observer.wait().map_err(AdapterError::Client),
            Err(rejection) => {
                let (returned, error) = rejection.into_parts();
                transaction = returned;
                if error.retry_advice() != RetryAdvice::RetrySafe || Instant::now() >= deadline {
                    return Err(AdapterError::Client(error));
                }
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

fn abort(mut transaction: Transaction<'_>, deadline: Instant) -> Result<(), AdapterError> {
    loop {
        match transaction.abort(remaining(deadline)?) {
            Ok(observer) => return observer.wait().map_err(AdapterError::Client),
            Err(rejection) => {
                let (returned, error) = rejection.into_parts();
                transaction = returned;
                if error.retry_advice() != RetryAdvice::RetrySafe || Instant::now() >= deadline {
                    return Err(AdapterError::Client(error));
                }
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

fn remaining(deadline: Instant) -> Result<Duration, AdapterError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(timeout_error("transaction command deadline elapsed"))
    } else {
        Ok(remaining)
    }
}

fn timeout_error(message: &str) -> AdapterError {
    AdapterError::Client(kafkars::KafkaError::new(ErrorKind::Timeout, message))
}
