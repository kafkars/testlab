//! Session interpreter translates protocol commands to one fixture state machine.

use std::io::{self, BufRead, Read, Write};

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, CommandEnvelope, PROTOCOL_VERSION,
};

use crate::AdapterError;
use crate::session_send;
use crate::state::AdapterState;

const MAX_COMMAND_BYTES: usize = 4 * 1024 * 1024;
const MAX_COMMAND_READ: u64 = 4 * 1024 * 1024 + 1;
/// Runs the adapter against process standard streams.
pub fn run_stdio() -> Result<(), AdapterError> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    run_session(stdin.lock(), stdout.lock())
}

pub(crate) fn run_session<R, W>(mut reader: R, mut writer: W) -> Result<(), AdapterError>
where
    R: BufRead,
    W: Write,
{
    let mut state = AdapterState::default();
    let mut line = String::new();
    loop {
        line.clear();
        let mut bounded = (&mut reader).take(MAX_COMMAND_READ);
        let bytes = bounded.read_line(&mut line)?;
        if bytes == 0 {
            return Err(AdapterError::UnexpectedEof);
        }
        if bytes > MAX_COMMAND_BYTES {
            return Err(AdapterError::CommandTooLarge);
        }
        if !line.ends_with('\n') {
            return Err(AdapterError::IncompleteCommand);
        }
        let envelope: CommandEnvelope = serde_json::from_str(line.trim_end())?;
        if envelope.protocol_version != PROTOCOL_VERSION {
            let protocol_version = envelope.protocol_version;
            return emit_fatal(
                &mut writer,
                envelope,
                AdapterError::ProtocolVersion(protocol_version),
            );
        }
        let finished = match dispatch(&mut state, &mut writer, envelope.clone()) {
            Ok(finished) => finished,
            Err(error) => return emit_fatal(&mut writer, envelope, error),
        };
        if finished {
            return Ok(());
        }
    }
}

#[allow(clippy::too_many_lines, reason = "exhaustive protocol dispatcher")]
fn dispatch<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    envelope: CommandEnvelope,
) -> Result<bool, AdapterError> {
    let command_id = envelope.command_id;
    match envelope.command {
        AdapterCommand::Hello {
            broker_endpoints, ..
        } => dispatch_hello(state, writer, command_id, broker_endpoints)?,
        AdapterCommand::CreateClient { client_id } => {
            state.create_client(client_id.clone())?;
            emit(
                writer,
                &AdapterEventEnvelope::new(command_id, AdapterEvent::ClientCreated { client_id }),
            )?;
        }
        AdapterCommand::AwaitClientReady { client_id } => {
            dispatch_client_ready(state, writer, command_id, client_id)?;
        }
        AdapterCommand::CreateProducer {
            client_id,
            producer_id,
        } => dispatch_create_producer(state, writer, command_id, client_id, producer_id)?,
        AdapterCommand::Send {
            producer_id,
            operation_id,
            record,
        } => session_send::dispatch_send(
            state,
            writer,
            command_id,
            &producer_id,
            operation_id,
            record,
        )?,
        AdapterCommand::SendBatch {
            producer_id,
            operations,
        } => session_send::dispatch_batch(state, writer, command_id, &producer_id, operations)?,
        command @ (AdapterCommand::StartConcurrentActors(_)
        | AdapterCommand::JoinConcurrentActors { .. }
        | AdapterCommand::CancelProducerSend(_)
        | AdapterCommand::CreateConfiguredClient(_)
        | AdapterCommand::ObserveClientMetrics(_)
        | AdapterCommand::CreateAssignedConsumer { .. }
        | AdapterCommand::AssignBeginning { .. }
        | AdapterCommand::AssignBeginningBatch(_)
        | AdapterCommand::ControlAssignedConsumer(_)
        | AdapterCommand::Receive { .. }
        | AdapterCommand::CloseAssignedConsumer { .. }
        | AdapterCommand::CreateGroupConsumer { .. }
        | AdapterCommand::GroupReceive { .. }
        | AdapterCommand::ObserveGroupAssignments(_)
        | AdapterCommand::GroupReceiveSet(_)
        | AdapterCommand::ControlGroupConsumer(_)
        | AdapterCommand::ShutdownGroupConsumer(_)
        | AdapterCommand::CloseGroupConsumer { .. }
        | AdapterCommand::CreateShareConsumer { .. }
        | AdapterCommand::ShareReceive { .. }
        | AdapterCommand::ShareAcknowledge { .. }
        | AdapterCommand::DropShareBatch { .. }
        | AdapterCommand::CloseShareConsumer { .. }
        | AdapterCommand::CreateTopic(_)
        | AdapterCommand::CreateTopicsBatch(_)
        | AdapterCommand::CreatePartitions(_)
        | AdapterCommand::DeleteTopic(_)
        | AdapterCommand::DescribeTopic(_)
        | AdapterCommand::ListTopics(_)
        | AdapterCommand::ListOffsets(_)
        | AdapterCommand::DeleteRecords(_)
        | AdapterCommand::DescribeTopicConfig(_)
        | AdapterCommand::AlterTopicConfig(_)
        | AdapterCommand::DescribeCluster(_)
        | AdapterCommand::ListConsumerGroups(_)
        | AdapterCommand::DescribeConsumerGroup(_)
        | AdapterCommand::ListConsumerGroupOffsets(_)
        | AdapterCommand::ListConsumerGroupOffsetsBatch(_)
        | AdapterCommand::ListConsumerGroupsOffsets(_)
        | AdapterCommand::AlterConsumerGroupOffset(_)
        | AdapterCommand::AlterConsumerGroupOffsets(_)
        | AdapterCommand::DeleteConsumerGroupOffset(_)
        | AdapterCommand::DeleteConsumerGroupOffsets(_)
        | AdapterCommand::DeleteConsumerGroup(_)
        | AdapterCommand::DescribeClassicGroups(_)
        | AdapterCommand::CreateTransactionalProducer { .. }
        | AdapterCommand::ExecuteTransaction { .. }
        | AdapterCommand::ExecuteTransactionalTransform(_)
        | AdapterCommand::FenceTransaction { .. }
        | AdapterCommand::CloseTransactionalProducer { .. }) => {
            return Err(AdapterError::Unsupported(
                crate::session_unsupported::reason(&command),
            ));
        }
        AdapterCommand::Flush { producer_id } => {
            state.require_producer(&producer_id)?;
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::FlushCompleted { producer_id },
                ),
            )?;
        }
        AdapterCommand::CloseProducer { producer_id } => {
            state.close_producer(&producer_id)?;
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::ProducerClosed { producer_id },
                ),
            )?;
        }
        AdapterCommand::ShutdownClient { client_id } => {
            state.shutdown_client(&client_id)?;
            emit(
                writer,
                &AdapterEventEnvelope::new(command_id, AdapterEvent::ClientShutdown { client_id }),
            )?;
        }
        command @ (AdapterCommand::Finish | AdapterCommand::Abort) => {
            return crate::session_end::dispatch(state, writer, command_id, &command);
        }
    }
    Ok(false)
}

fn dispatch_create_producer<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: testlab_schema::CommandId,
    client_id: testlab_schema::ClientId,
    producer_id: testlab_schema::ProducerId,
) -> Result<(), AdapterError> {
    state.create_producer(client_id, producer_id.clone())?;
    emit(
        writer,
        &AdapterEventEnvelope::new(command_id, AdapterEvent::ProducerCreated { producer_id }),
    )
}

fn dispatch_hello<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: testlab_schema::CommandId,
    broker_endpoints: Vec<String>,
) -> Result<(), AdapterError> {
    state.hello(broker_endpoints)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::Ready {
                descriptor: crate::session_descriptor::descriptor()?,
            },
        ),
    )
}

fn dispatch_client_ready<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: testlab_schema::CommandId,
    client_id: testlab_schema::ClientId,
) -> Result<(), AdapterError> {
    state.require_client(&client_id)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(command_id, AdapterEvent::ClientReady { client_id }),
    )
}

pub(crate) fn emit<W: Write>(
    writer: &mut W,
    event: &AdapterEventEnvelope,
) -> Result<(), AdapterError> {
    serde_json::to_writer(&mut *writer, event)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

fn emit_fatal<W: Write>(
    writer: &mut W,
    envelope: CommandEnvelope,
    error: AdapterError,
) -> Result<(), AdapterError> {
    let event = AdapterEventEnvelope::new(
        envelope.command_id,
        AdapterEvent::Fatal {
            code: error.code().to_owned(),
            diagnostic: error.to_string(),
        },
    );
    let _ = emit(writer, &event);
    Err(error)
}
