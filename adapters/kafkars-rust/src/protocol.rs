//! Protocol interpreter translates commands into packaged Kafkars public calls.

use std::collections::BTreeSet;
use std::io::{self, BufRead, Read, Write};

use testlab_schema::{
    AdapterCommand, AdapterDescriptor, AdapterEvent, AdapterEventEnvelope, AdapterId, Capability,
    CommandEnvelope, CommandId, OperationId, PROTOCOL_VERSION, ProducerId, RecordSpec,
    TerminalStatus,
};

use crate::AdapterError;
use crate::normalize;
use crate::state::AdapterState;

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
            broker_endpoint, ..
        } => {
            state.hello(broker_endpoint)?;
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::Ready {
                        descriptor: descriptor()?,
                    },
                ),
            )?;
        }
        AdapterCommand::CreateClient { client_id } => {
            state.create_client(client_id.clone())?;
            emit(
                writer,
                &AdapterEventEnvelope::new(command_id, AdapterEvent::ClientCreated { client_id }),
            )?;
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
        } => dispatch_send(
            state,
            writer,
            command_id,
            &producer_id,
            operation_id,
            record,
        )?,
        AdapterCommand::Flush { producer_id } => {
            state
                .producer(&producer_id)?
                .flush()
                .wait()
                .map_err(AdapterError::Client)?;
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

fn dispatch_send<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    producer_id: &ProducerId,
    operation_id: OperationId,
    record: RecordSpec,
) -> Result<(), AdapterError> {
    let producer = state.producer(producer_id)?;
    let record = normalize::record(record)?;
    let delivery = match producer.try_send(record) {
        Ok(delivery) => delivery,
        Err(rejection) => {
            let (_, error) = rejection.into_parts();
            eprintln!("Kafkars rejected operation {operation_id}: {error}");
            return emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::OperationRejected {
                        operation_id,
                        code: normalize::error_code(&error),
                    },
                ),
            );
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
    let (status, code, offset) = match delivery.wait() {
        Ok(metadata) => (TerminalStatus::Acknowledged, None, Some(metadata.offset())),
        Err(error) => {
            eprintln!("Kafkars delivery failed for {operation_id}: {error}");
            let failure = normalize::delivery_failure(&error);
            (failure.status, Some(failure.code), None)
        }
    };
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::OperationTerminal {
                operation_id,
                status,
                code,
                offset,
            },
        ),
    )
}

fn descriptor() -> Result<AdapterDescriptor, AdapterError> {
    Ok(AdapterDescriptor {
        id: AdapterId::new("kafkars-rust")?,
        implementation: "packaged kafkars Rust client".to_owned(),
        version: "0.0.1".to_owned(),
        protocol_version: PROTOCOL_VERSION,
        capabilities: BTreeSet::from([Capability::Producer, Capability::Lifecycle]),
    })
}

fn emit<W: Write>(writer: &mut W, event: &AdapterEventEnvelope) -> Result<(), AdapterError> {
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
