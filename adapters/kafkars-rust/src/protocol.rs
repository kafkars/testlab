//! Protocol interpreter translates commands into packaged Kafkars public calls.

use std::collections::BTreeSet;
use std::io::{self, BufRead, Read, Write};

use testlab_schema::{
    AdapterCommand, AdapterDescriptor, AdapterEvent, AdapterEventEnvelope, AdapterId, Capability,
    CommandEnvelope, PROTOCOL_VERSION,
};

use crate::AdapterError;
use crate::normalize;
use crate::protocol_admin;
use crate::protocol_consumer;
use crate::protocol_group;
use crate::protocol_lifecycle;
use crate::protocol_send;
#[cfg(kafkars_share_candidate)]
use crate::protocol_share;
use crate::state::AdapterState;
use crate::transaction_execute;
use crate::transaction_fence;

const MAX_COMMAND_BYTES: usize = 4 * 1024 * 1024;
const MAX_COMMAND_READ: u64 = 4 * 1024 * 1024 + 1;

/// Runs the packaged Kafkars adapter against process standard streams.
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
            let version = envelope.protocol_version;
            return emit_fatal(
                &mut writer,
                envelope,
                AdapterError::ProtocolVersion(version),
            );
        }
        let finished = match dispatch(&mut state, &mut writer, envelope.clone()) {
            Ok(finished) => finished,
            Err(error) if error.client_failure().is_some() => {
                return emit_client_failure(&mut writer, envelope, error);
            }
            Err(error) => return emit_fatal(&mut writer, envelope, error),
        };
        if finished {
            return Ok(());
        }
    }
}

fn dispatch<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    envelope: CommandEnvelope,
) -> Result<bool, AdapterError> {
    let command_id = envelope.command_id;
    match envelope.command {
        AdapterCommand::Hello {
            broker_endpoints,
            security,
            ..
        } => dispatch_hello(state, writer, command_id, broker_endpoints, security)?,
        AdapterCommand::CreateClient { client_id } => {
            state.create_client(client_id.clone())?;
            emit(
                writer,
                &AdapterEventEnvelope::new(command_id, AdapterEvent::ClientCreated { client_id }),
            )?;
        }
        AdapterCommand::AwaitClientReady { client_id } => {
            state.await_client_ready(&client_id)?;
            emit(
                writer,
                &AdapterEventEnvelope::new(command_id, AdapterEvent::ClientReady { client_id }),
            )?;
        }
        AdapterCommand::CreateProducer {
            client_id,
            producer_id,
        } => dispatch_create_producer(state, writer, command_id, client_id, producer_id)?,
        AdapterCommand::Send {
            producer_id,
            operation_id,
            record,
        } => protocol_send::dispatch_send(
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
        } => protocol_send::dispatch_batch(state, writer, command_id, &producer_id, operations)?,
        command @ (AdapterCommand::CreateAssignedConsumer { .. }
        | AdapterCommand::AssignBeginning { .. }
        | AdapterCommand::Receive { .. }
        | AdapterCommand::CloseAssignedConsumer { .. }) => {
            protocol_consumer::dispatch(state, writer, command_id, command)?;
        }
        command @ (AdapterCommand::CreateGroupConsumer { .. }
        | AdapterCommand::GroupReceive { .. }
        | AdapterCommand::CloseGroupConsumer { .. }) => {
            protocol_group::dispatch(state, writer, command_id, command)?;
        }
        #[cfg(kafkars_share_candidate)]
        command @ (AdapterCommand::CreateShareConsumer { .. }
        | AdapterCommand::ShareReceive { .. }
        | AdapterCommand::ShareAcknowledge { .. }
        | AdapterCommand::DropShareBatch { .. }
        | AdapterCommand::CloseShareConsumer { .. }) => {
            protocol_share::dispatch(state, writer, command_id, command)?;
        }
        #[cfg(not(kafkars_share_candidate))]
        AdapterCommand::CreateShareConsumer { .. }
        | AdapterCommand::ShareReceive { .. }
        | AdapterCommand::ShareAcknowledge { .. }
        | AdapterCommand::DropShareBatch { .. }
        | AdapterCommand::CloseShareConsumer { .. } => {
            return Err(AdapterError::State(
                "published adapter does not expose the candidate share capability".to_owned(),
            ));
        }
        command @ (AdapterCommand::CreateTopic { .. }
        | AdapterCommand::CreatePartitions { .. }
        | AdapterCommand::DescribeTopic { .. }
        | AdapterCommand::ListTopics { .. }
        | AdapterCommand::ListOffsets { .. }) => {
            protocol_admin::dispatch(state, writer, command_id, command)?;
        }
        command @ (AdapterCommand::CreateTransactionalProducer { .. }
        | AdapterCommand::ExecuteTransaction { .. }
        | AdapterCommand::CloseTransactionalProducer { .. }) => {
            transaction_execute::dispatch(state, writer, command_id, command)?;
        }
        command @ AdapterCommand::FenceTransaction { .. } => {
            transaction_fence::dispatch(state, writer, command_id, command)?;
        }
        command @ (AdapterCommand::Flush { .. }
        | AdapterCommand::CloseProducer { .. }
        | AdapterCommand::ShutdownClient { .. }
        | AdapterCommand::Finish
        | AdapterCommand::Abort) => {
            return protocol_lifecycle::dispatch(state, writer, command_id, command);
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
    security: testlab_schema::AdapterSecurity,
) -> Result<(), AdapterError> {
    state.hello(broker_endpoints, security)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::Ready {
                descriptor: descriptor()?,
            },
        ),
    )
}

fn descriptor() -> Result<AdapterDescriptor, AdapterError> {
    let capabilities = BTreeSet::from([
        Capability::Producer,
        Capability::ProducerBatch,
        Capability::Lifecycle,
        Capability::ClientReadiness,
        Capability::AssignedConsumer,
        Capability::ConsumerGroups,
        Capability::ConsumerProtocolGroups,
        Capability::Admin,
        Capability::Transactions,
    ]);
    #[cfg(kafkars_share_candidate)]
    let capabilities = {
        let mut capabilities = capabilities;
        capabilities.insert(Capability::ShareConsumer);
        capabilities
    };
    Ok(AdapterDescriptor {
        id: AdapterId::new("kafkars-rust")?,
        implementation: "packaged kafkars Rust client".to_owned(),
        version: "0.0.1".to_owned(),
        protocol_version: PROTOCOL_VERSION,
        capabilities,
    })
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

pub(super) fn emit_client_failure<W: Write>(
    writer: &mut W,
    envelope: CommandEnvelope,
    error: AdapterError,
) -> Result<(), AdapterError> {
    let Some(client_error) = error.client_failure() else {
        return Err(error);
    };
    eprintln!(
        "Kafkars command {} failed: {client_error}",
        envelope.command_id
    );
    emit(
        writer,
        &AdapterEventEnvelope::new(
            envelope.command_id,
            AdapterEvent::CommandFailed {
                code: normalize::error_code(client_error),
                diagnostic: client_error.to_string(),
            },
        ),
    )
}
