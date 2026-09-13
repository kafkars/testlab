//! Group-consumer protocol translation preserves public receive and checkpoint ownership.

use std::io::Write;
use std::time::{Duration, Instant};

use crate::kafkars_api::{
    ConsumerBatch, ConsumerCommitAdmissionError,
    GroupMembershipEpoch as PublicGroupMembershipEpoch, GroupMetadata, RetryAdvice,
};
use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, CommandId, ConsumedRecord, ConsumerId,
    GroupMembershipEpoch, OperationId,
};

use crate::AdapterError;
use crate::admission_retry::retry_owned_until;
use crate::group_consumers::GroupConsumerRegistration;
pub(crate) use crate::group_receive_events::receive_batch;
use crate::protocol::emit;
pub(crate) use crate::protocol_group_record::normalize_record;
use crate::state::AdapterState;
const POLL_SLICE: Duration = Duration::from_millis(10);
#[allow(
    clippy::too_many_lines,
    reason = "exhaustive group-consumer routing keeps each public lifecycle command explicit"
)]
pub(crate) fn dispatch<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    match command {
        AdapterCommand::CreateGroupConsumer {
            client_id,
            consumer_id,
            group_id,
            topics,
            protocol,
            configuration,
        } => {
            let observation = state.create_group_consumer(GroupConsumerRegistration {
                client_id,
                consumer_id,
                group_id,
                topics,
                protocol,
                configuration,
            })?;
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::GroupConsumerCreated(observation),
                ),
            )
        }
        AdapterCommand::GroupReceive {
            consumer_id,
            method,
            checkpoint_method,
            receive_id,
            processing_acknowledgement_delay_ms,
            processed_record_count,
            timeout_ms,
        } => receive(
            state,
            writer,
            command_id,
            &consumer_id,
            method,
            checkpoint_method,
            receive_id,
            processing_acknowledgement_delay_ms,
            processed_record_count,
            timeout_ms,
        ),
        AdapterCommand::ObserveGroupAssignments(command) => {
            crate::group_assignment_observe::observe(state, writer, command_id, command)
        }
        AdapterCommand::GroupReceiveSet(command) => {
            crate::group_receive_set::receive(state, writer, command_id, command)
        }
        AdapterCommand::ControlGroupConsumer(command) => {
            let completion = testlab_schema::GroupConsumerControlCompletion {
                operation_id: command.operation_id.clone(),
                consumer_id: command.consumer_id.clone(),
                control: command.control.kind(),
            };
            state.control_group_consumer(&command)?;
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::GroupConsumerControlCompleted(completion),
                ),
            )
        }
        AdapterCommand::ShutdownGroupConsumer(command) => {
            let completion = testlab_schema::GroupConsumerShutdownCompletion {
                operation_id: command.operation_id.clone(),
                consumer_id: command.consumer_id.clone(),
                request_count: command.request_count,
            };
            crate::group_consumer_shutdown::shutdown(state, &command)?;
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::GroupConsumerShutdownCompleted(completion),
                ),
            )
        }
        AdapterCommand::CloseGroupConsumer { consumer_id } => {
            state.close_group_consumer(&consumer_id)?;
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::GroupConsumerClosed { consumer_id },
                ),
            )
        }
        AdapterCommand::AbandonGroupConsumer(abandonment) => {
            state.abandon_group_consumer(&abandonment.consumer_id)?;
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::GroupConsumerAbandoned(abandonment),
                ),
            )
        }
        _ => Err(AdapterError::ConsumerRecord(
            "non-group command reached group dispatcher".to_owned(),
        )),
    }
}
#[allow(
    clippy::too_many_arguments,
    reason = "the receive boundary retains every command identity and checkpoint control"
)]
fn receive<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    consumer_id: &ConsumerId,
    method: testlab_schema::GroupConsumerReceiveMethod,
    checkpoint_method: testlab_schema::GroupCheckpointMethod,
    receive_id: OperationId,
    processing_acknowledgement_delay_ms: u64,
    processed_record_count: Option<usize>,
    timeout_ms: u64,
) -> Result<(), AdapterError> {
    let timeout = Duration::from_millis(timeout_ms);
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| AdapterError::ConsumerRecord("receive deadline overflow".to_owned()))?;
    let (records, committed) = match receive_batch(state, consumer_id, method, deadline)? {
        Some(batch) => (
            commit_batch(
                state,
                consumer_id,
                batch,
                checkpoint_method,
                processing_acknowledgement_delay_ms,
                processed_record_count,
                deadline,
            )?,
            true,
        ),
        None => (Vec::new(), false),
    };
    let group_epoch = public_group_epoch(state, consumer_id, deadline)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::GroupReceiveCompleted {
                receive_id,
                records,
                committed,
                group_epoch,
            },
        ),
    )
}
pub(crate) fn public_group_epoch(
    state: &mut AdapterState,
    consumer_id: &ConsumerId,
    deadline: Instant,
) -> Result<Option<GroupMembershipEpoch>, AdapterError> {
    Ok(public_group_metadata(state, consumer_id, deadline)?
        .map(|metadata| normalize_group_epoch(metadata.membership_epoch())))
}

pub(crate) fn public_group_metadata(
    state: &mut AdapterState,
    consumer_id: &ConsumerId,
    deadline: Instant,
) -> Result<Option<GroupMetadata>, AdapterError> {
    loop {
        match state.group_consumer_mut(consumer_id)?.group_metadata() {
            Ok(Some(metadata)) => return Ok(Some(metadata)),
            Ok(None) => {}
            Err(error) if error.retry_advice() == RetryAdvice::RetrySafe => {}
            Err(error) => return Err(AdapterError::Client(error)),
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        std::thread::sleep(POLL_SLICE);
    }
}

pub(crate) fn normalize_group_epoch(epoch: PublicGroupMembershipEpoch) -> GroupMembershipEpoch {
    match epoch {
        PublicGroupMembershipEpoch::Classic { generation_id } => {
            GroupMembershipEpoch::Classic { generation_id }
        }
        PublicGroupMembershipEpoch::Consumer { member_epoch } => {
            GroupMembershipEpoch::Consumer { member_epoch }
        }
    }
}

pub(crate) fn commit_batch(
    state: &mut AdapterState,
    consumer_id: &ConsumerId,
    batch: ConsumerBatch,
    checkpoint_method: testlab_schema::GroupCheckpointMethod,
    processing_acknowledgement_delay_ms: u64,
    processed_record_count: Option<usize>,
    deadline: Instant,
) -> Result<Vec<ConsumedRecord>, AdapterError> {
    let records = batch
        .records()
        .map(|record| normalize_record(&record))
        .collect::<Result<Vec<_>, _>>()?;
    let consumer = state.group_consumer_mut(consumer_id)?;
    let mut checkpoint = crate::group_checkpoint::checkpoint(
        consumer,
        batch,
        checkpoint_method,
        processing_acknowledgement_delay_ms,
        processed_record_count,
        deadline,
    )?;
    loop {
        let commit = retry_owned_until(
            deadline,
            checkpoint,
            |checkpoint| {
                consumer
                    .try_commit(
                        checkpoint,
                        deadline.saturating_duration_since(Instant::now()),
                    )
                    .map_err(ConsumerCommitAdmissionError::into_parts)
            },
            |error| error.retry_advice() == RetryAdvice::RetrySafe,
        )
        .map_err(|(_, error)| AdapterError::Client(error))?;
        match commit.wait() {
            Ok(()) => return Ok(records),
            Err(failure) => {
                let (returned, error) = failure.into_parts();
                checkpoint = match returned {
                    Some(checkpoint)
                        if error.retry_advice() == RetryAdvice::RetrySafe
                            && Instant::now() < deadline =>
                    {
                        checkpoint
                    }
                    _ => return Err(AdapterError::Client(error)),
                };
                std::thread::sleep(
                    POLL_SLICE.min(deadline.saturating_duration_since(Instant::now())),
                );
                if Instant::now() >= deadline {
                    return Err(AdapterError::Client(error));
                }
            }
        }
    }
}
