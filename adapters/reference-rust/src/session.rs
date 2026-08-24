//! Session interpreter translates protocol commands to one fixture state machine.

use std::collections::BTreeSet;
use std::io::{self, BufRead, Read, Write};

use testlab_schema::{
    AdapterCommand, AdapterDescriptor, AdapterEvent, AdapterEventEnvelope, AdapterId, Capability,
    CommandEnvelope, PROTOCOL_VERSION,
};
use thiserror::Error;

use crate::session_send;
use crate::state::{AdapterState, StateError};

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
        } => {
            state.create_producer(client_id, producer_id.clone())?;
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::ProducerCreated { producer_id },
                ),
            )?;
        }
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
        command @ (AdapterCommand::CreateAssignedConsumer { .. }
        | AdapterCommand::AssignBeginning { .. }
        | AdapterCommand::Receive { .. }
        | AdapterCommand::CloseAssignedConsumer { .. }
        | AdapterCommand::CreateGroupConsumer { .. }
        | AdapterCommand::GroupReceive { .. }
        | AdapterCommand::CloseGroupConsumer { .. }
        | AdapterCommand::CreateTopic { .. }
        | AdapterCommand::CreatePartitions { .. }
        | AdapterCommand::CreateTransactionalProducer { .. }
        | AdapterCommand::ExecuteTransaction { .. }
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
        AdapterCommand::Finish => {
            state.finish()?;
            emit(
                writer,
                &AdapterEventEnvelope::new(command_id, AdapterEvent::Finished),
            )?;
            return Ok(true);
        }
    }
    Ok(false)
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
                descriptor: descriptor()?,
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

fn descriptor() -> Result<AdapterDescriptor, AdapterError> {
    Ok(AdapterDescriptor {
        id: AdapterId::new("reference-rust")?,
        implementation: "testlab reference Rust adapter".to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        protocol_version: PROTOCOL_VERSION,
        capabilities: BTreeSet::from([
            Capability::Producer,
            Capability::ProducerBatch,
            Capability::Lifecycle,
            Capability::ClientReadiness,
            Capability::ModelBroker,
        ]),
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

/// Reference adapter protocol or lifecycle failure.
#[derive(Debug, Error)]
pub enum AdapterError {
    /// Standard stream or model transport I/O failed.
    #[error("adapter I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// One control message was malformed.
    #[error("adapter JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    /// One stable identifier was invalid.
    #[error("adapter identity failed: {0}")]
    Id(#[from] testlab_schema::IdError),
    /// Public fixture lifecycle was invalid.
    #[error("adapter state failed: {0}")]
    State(String),
    /// One batch command did not contain an operation.
    #[error("adapter batch failed: {0}")]
    Batch(String),
    /// A command reached a capability this adapter does not declare.
    #[error("unsupported adapter command: {0}")]
    Unsupported(&'static str),
    /// The harness used an unsupported protocol version.
    #[error("unsupported protocol version {0}")]
    ProtocolVersion(u16),
    /// A command exceeded the bounded input size.
    #[error("command exceeded {MAX_COMMAND_BYTES} bytes")]
    CommandTooLarge,
    /// One command ended without its JSON Lines delimiter.
    #[error("command ended without a newline delimiter")]
    IncompleteCommand,
    /// The harness closed stdin before `finish` settled.
    #[error("stdin closed before finish")]
    UnexpectedEof,
}

impl AdapterError {
    fn code(&self) -> &'static str {
        match self {
            Self::Io(_) => "adapter_io",
            Self::Json(_) => "adapter_json",
            Self::Id(_) => "adapter_identity",
            Self::State(_) => "adapter_state",
            Self::Batch(_) => "adapter_batch",
            Self::Unsupported(_) => "adapter_unsupported",
            Self::ProtocolVersion(_) => "protocol_version",
            Self::CommandTooLarge => "command_too_large",
            Self::IncompleteCommand => "incomplete_command",
            Self::UnexpectedEof => "unexpected_eof",
        }
    }
}

impl From<StateError> for AdapterError {
    fn from(error: StateError) -> Self {
        Self::State(error.to_string())
    }
}
