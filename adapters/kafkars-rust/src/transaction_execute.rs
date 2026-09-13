//! Transaction commands preserve public admission, staging, and end disposition.

use std::io::Write;
use std::thread;
use std::time::{Duration, Instant};

use crate::kafkars_api::{RetryAdvice, TopicUuid, Transaction, TransactionalProducer};
use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, BatchRecord, CommandId, OperationId,
    TerminalStatus, TransactionDisposition, TransactionSendMethod,
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
        command @ AdapterCommand::ExecuteTransaction {
            disposition: TransactionDisposition::AdminPartitionAbort,
            ..
        } => crate::transaction_admin_abort::dispatch(state, writer, command_id, command),
        AdapterCommand::CreateTransactionalProducer {
            client_id,
            producer_id,
            transactional_id,
            transaction_timeout_ms,
            initialization_timeout_ms,
        } => {
            let observation = state.create_transactional_producer(
                client_id,
                producer_id,
                &transactional_id,
                Duration::from_millis(transaction_timeout_ms),
                Duration::from_millis(initialization_timeout_ms),
            )?;
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::TransactionalProducerCreated(observation),
                ),
            )
        }
        AdapterCommand::ExecuteTransaction {
            producer_id,
            transaction_id,
            operations,
            method,
            disposition,
            validate_topic_uuids,
            timeout_ms,
        } => {
            if validate_topic_uuids && disposition != TransactionDisposition::Commit {
                return Err(AdapterError::TransactionResult(
                    "topic UUID validation requires commit disposition".to_owned(),
                ));
            }
            let deadline = Instant::now()
                .checked_add(Duration::from_millis(timeout_ms))
                .ok_or_else(|| {
                    crate::transaction_end::timeout_error("transaction deadline overflow")
                })?;
            let validation = crate::transaction_topic_validation::TopicValidation::resolve(
                state,
                &producer_id,
                &transaction_id,
                &operations,
                validate_topic_uuids,
                deadline,
            )?;
            execute(
                state.transactional_producer_mut(&producer_id)?,
                writer,
                command_id,
                transaction_id,
                operations,
                method,
                disposition,
                validation,
                deadline,
            )
        }
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
    method: TransactionSendMethod,
    disposition: TransactionDisposition,
    validation: crate::transaction_topic_validation::TopicValidation,
    deadline: Instant,
) -> Result<(), AdapterError> {
    loop {
        match producer.begin() {
            Ok(transaction) => {
                return execute_started(
                    transaction,
                    writer,
                    command_id,
                    transaction_id,
                    operations,
                    method,
                    disposition,
                    validation,
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
    method: TransactionSendMethod,
    disposition: TransactionDisposition,
    validation: crate::transaction_topic_validation::TopicValidation,
    deadline: Instant,
) -> Result<(), AdapterError> {
    match method {
        TransactionSendMethod::Send => {
            for operation in operations {
                let topic_uuid = validation.topic_uuid(&operation.record.topic)?;
                send(
                    &mut transaction,
                    writer,
                    &command_id,
                    operation,
                    topic_uuid,
                    deadline,
                )?;
            }
        }
        TransactionSendMethod::SendBatch => {
            let topic_uuid = operations
                .first()
                .map(|operation| validation.topic_uuid(&operation.record.topic))
                .transpose()?
                .flatten();
            crate::transaction_send_batch::send(
                &mut transaction,
                writer,
                &command_id,
                operations,
                topic_uuid,
                deadline,
            )?;
        }
    }
    validation.seal(&mut transaction, deadline)?;
    let validated_topic_ids = validation.topic_ids();
    crate::transaction_end::end(transaction, disposition, deadline)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::TransactionCompleted {
                transaction_id,
                disposition,
                validated_topic_ids,
            },
        ),
    )
}

pub(crate) fn send<W: Write>(
    transaction: &mut Transaction<'_>,
    writer: &mut W,
    command_id: &CommandId,
    operation: BatchRecord,
    topic_uuid: Option<TopicUuid>,
    deadline: Instant,
) -> Result<(), AdapterError> {
    let operation_id = operation.operation_id;
    let mut record = normalize::record(operation.record)?;
    if let Some(topic_uuid) = topic_uuid {
        record = record.expected_topic_uuid(topic_uuid);
    }
    let observer = loop {
        match transaction.send(record, crate::transaction_end::remaining(deadline)?) {
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
    let (status, code, partition, offset, timestamp_millis, terminal_error) = match observer.wait()
    {
        Ok(metadata) => (
            TerminalStatus::TransactionStaged,
            None,
            Some(metadata.partition()),
            Some(metadata.offset()),
            metadata.timestamp_milliseconds(),
            None,
        ),
        Err(error) => {
            let failure = normalize::delivery_failure(&error);
            (
                failure.status,
                Some(failure.code),
                None,
                None,
                None,
                Some(error),
            )
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
                partition,
                offset,
                timestamp_millis,
                receipt: None,
            },
        ),
    )?;
    terminal_error.map_or(Ok(()), |error| Err(AdapterError::Client(error)))
}
