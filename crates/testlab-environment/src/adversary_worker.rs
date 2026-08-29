//! External adversary worker owns the Kafka listener and JSON Lines control loop.

use std::io::{self, BufRead};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use testlab_schema::{ADVERSARY_PROTOCOL_VERSION, AdversaryControlEnvelope, AdversaryEvent};

use crate::adversary_connection::{ConnectionContext, serve};
use crate::adversary_output::EventWriter;
use crate::adversary_state::AdversaryState;

const MAX_CONTROL_LINE_BYTES: usize = 64 * 1024;

/// Runs the protocol-only child process until its control stdin reaches EOF.
pub fn run_adversary_worker(topic: &str) -> Result<(), String> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|error| format!("bind adversary listener: {error}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|error| format!("configure adversary listener: {error}"))?;
    let endpoint = listener
        .local_addr()
        .map_err(|error| format!("read adversary listener address: {error}"))?
        .to_string();
    let state = Arc::new(Mutex::new(AdversaryState::default()));
    let output = EventWriter::default();
    let stopping = Arc::new(AtomicBool::new(false));
    let acceptor = spawn_acceptor(
        listener,
        endpoint.clone(),
        topic.to_owned(),
        Arc::clone(&state),
        output.clone(),
        Arc::clone(&stopping),
    );
    output.emit(&AdversaryEvent::Ready {
        protocol_version: ADVERSARY_PROTOCOL_VERSION,
        endpoint,
    })?;
    let controls = read_controls(&state, &output);
    stopping.store(true, Ordering::Release);
    let accepted = acceptor
        .join()
        .map_err(|_| "adversary acceptor panicked".to_owned())?;
    controls?;
    accepted?;
    let state = state
        .lock()
        .map_err(|_| "adversary state lock was poisoned".to_owned())?;
    if let Some(diagnostic) = state.fatal() {
        return Err(diagnostic.to_owned());
    }
    let unconsumed = state.unconsumed_controls();
    if !unconsumed.is_empty() {
        return Err(format!(
            "adversary controls were not exercised: {}",
            unconsumed
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(())
}

fn spawn_acceptor(
    listener: TcpListener,
    endpoint: String,
    topic: String,
    state: Arc<Mutex<AdversaryState>>,
    output: EventWriter,
    stopping: Arc<AtomicBool>,
) -> thread::JoinHandle<Result<(), String>> {
    thread::spawn(move || {
        let next_connection = AtomicU64::new(0);
        let next_observation = Arc::new(AtomicU64::new(0));
        let mut connections = Vec::new();
        while !stopping.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((peer, _)) => {
                    let connection = next_connection.fetch_add(1, Ordering::AcqRel);
                    let context = ConnectionContext {
                        connection,
                        endpoint: endpoint.clone(),
                        topic: topic.clone(),
                        state: Arc::clone(&state),
                        output: output.clone(),
                        next_observation: Arc::clone(&next_observation),
                        stopping: Arc::clone(&stopping),
                    };
                    let state = Arc::clone(&state);
                    let output = output.clone();
                    connections.push(thread::spawn(move || {
                        if let Err(diagnostic) = serve(peer, &context) {
                            record_fatal(&state, &output, "connection_failed", diagnostic);
                        }
                    }));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => {
                    let diagnostic = format!("accept Kafka connection: {error}");
                    record_fatal(&state, &output, "accept_failed", diagnostic.clone());
                    return Err(diagnostic);
                }
            }
        }
        for connection in connections {
            connection
                .join()
                .map_err(|_| "adversary connection worker panicked".to_owned())?;
        }
        Ok(())
    })
}

fn read_controls(state: &Arc<Mutex<AdversaryState>>, output: &EventWriter) -> Result<(), String> {
    let stdin = io::stdin();
    let mut reader = stdin.lock();
    loop {
        let Some(line) = read_bounded_line(&mut reader)? else {
            return Ok(());
        };
        let envelope: AdversaryControlEnvelope = serde_json::from_str(&line)
            .map_err(|error| format!("decode adversary control: {error}"))?;
        if envelope.protocol_version != ADVERSARY_PROTOCOL_VERSION {
            return Err(format!(
                "unsupported adversary protocol version {}",
                envelope.protocol_version
            ));
        }
        let operation_id = envelope.control.operation_id.clone();
        state
            .lock()
            .map_err(|_| "adversary state lock was poisoned".to_owned())?
            .arm(envelope.control)?;
        output.emit(&AdversaryEvent::Armed {
            protocol_version: ADVERSARY_PROTOCOL_VERSION,
            operation_id,
        })?;
    }
}

fn read_bounded_line<R: BufRead>(reader: &mut R) -> Result<Option<String>, String> {
    let mut bytes = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .map_err(|error| format!("read adversary control: {error}"))?;
        if available.is_empty() {
            return if bytes.is_empty() {
                Ok(None)
            } else {
                Err("adversary control line was incomplete".to_owned())
            };
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        bytes.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);
        if bytes.len() > MAX_CONTROL_LINE_BYTES {
            return Err("adversary control line exceeded its bound".to_owned());
        }
        if newline.is_some() {
            let _newline = bytes.pop();
            return String::from_utf8(bytes)
                .map(Some)
                .map_err(|error| format!("adversary control was not UTF-8: {error}"));
        }
    }
}

fn record_fatal(
    state: &Arc<Mutex<AdversaryState>>,
    output: &EventWriter,
    code: &str,
    diagnostic: String,
) {
    if let Ok(mut state) = state.lock() {
        state.fail(diagnostic.clone());
    }
    let _ignored = output.emit(&AdversaryEvent::Fatal {
        protocol_version: ADVERSARY_PROTOCOL_VERSION,
        code: code.to_owned(),
        diagnostic,
    });
}
