//! TCP server lifecycle keeps observations independent from adapter events.

use std::fmt::{Debug, Formatter};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use testlab_schema::{BrokerBehavior, BrokerObservation};
use thiserror::Error;

use crate::state::{BrokerAction, BrokerState};
use crate::{ModelBrokerRequest, ModelBrokerResponse};

const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;
const MAX_REQUEST_READ: u64 = 4 * 1024 * 1024 + 1;
const IO_TIMEOUT: Duration = Duration::from_secs(2);

/// Running independent model-broker fixture.
pub struct RunningBroker {
    endpoint: String,
    state: Arc<Mutex<BrokerState>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl RunningBroker {
    /// Starts a broker on an ephemeral loopback port.
    pub fn start() -> Result<Self, BrokerError> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let endpoint = listener.local_addr()?.to_string();
        let state = Arc::new(Mutex::new(BrokerState::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_state = Arc::clone(&state);
        let thread_stop = Arc::clone(&stop);
        let thread = thread::Builder::new()
            .name("testlab-model-broker".to_owned())
            .spawn(move || serve(&listener, &thread_state, &thread_stop))?;
        Ok(Self {
            endpoint,
            state,
            stop,
            thread: Some(thread),
        })
    }

    /// Returns the loopback endpoint adapters should use.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Selects the behavior for the next request.
    pub fn set_next_behavior(&self, behavior: BrokerBehavior) -> Result<(), BrokerError> {
        lock(&self.state)?.push_behavior(behavior);
        Ok(())
    }

    /// Returns a stable snapshot of all external observations.
    pub fn observations(&self) -> Result<Vec<BrokerObservation>, BrokerError> {
        Ok(lock(&self.state)?.observations())
    }

    /// Returns a background server failure, if one occurred.
    pub fn failure(&self) -> Result<Option<String>, BrokerError> {
        Ok(lock(&self.state)?.failure())
    }

    /// Stops the broker thread and waits for ownership to settle.
    pub fn shutdown(mut self) -> Result<(), BrokerError> {
        self.stop_and_join()
    }

    fn stop_and_join(&mut self) -> Result<(), BrokerError> {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| BrokerError::ThreadPanicked)?;
        }
        Ok(())
    }
}

impl Debug for RunningBroker {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RunningBroker")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl Drop for RunningBroker {
    fn drop(&mut self) {
        let _ = self.stop_and_join();
    }
}

fn serve(listener: &TcpListener, state: &Arc<Mutex<BrokerState>>, stop: &AtomicBool) {
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _)) => {
                if let Err(error) = handle_connection(stream, state) {
                    record_failure(state, error.to_string());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => {
                record_failure(state, format!("accept failed: {error}"));
                break;
            }
        }
    }
}

fn handle_connection(
    mut stream: TcpStream,
    state: &Arc<Mutex<BrokerState>>,
) -> Result<(), BrokerError> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    let mut reader = BufReader::new(&mut stream).take(MAX_REQUEST_READ);
    let mut line = String::new();
    let bytes = reader.read_line(&mut line)?;
    drop(reader);
    if bytes == 0 {
        return Ok(());
    }
    if bytes > MAX_REQUEST_BYTES {
        return Err(BrokerError::RequestTooLarge);
    }
    if !line.ends_with('\n') {
        return Err(BrokerError::IncompleteRequest);
    }
    let request: ModelBrokerRequest = serde_json::from_str(line.trim_end())?;
    let action = lock(state)?.apply(request).map_err(BrokerError::State)?;
    if let BrokerAction::Respond(response) = action {
        write_response(&mut stream, &response)?;
    }
    Ok(())
}

fn write_response(
    stream: &mut TcpStream,
    response: &ModelBrokerResponse,
) -> Result<(), BrokerError> {
    serde_json::to_writer(&mut *stream, response)?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    Ok(())
}

fn lock(state: &Arc<Mutex<BrokerState>>) -> Result<MutexGuard<'_, BrokerState>, BrokerError> {
    state.lock().map_err(|_| BrokerError::StatePoisoned)
}

fn record_failure(state: &Arc<Mutex<BrokerState>>, diagnostic: String) {
    if let Ok(mut state) = state.lock() {
        state.set_failure(diagnostic);
    }
}

/// Model-broker startup, protocol, or ownership failure.
#[derive(Debug, Error)]
pub enum BrokerError {
    /// Operating-system I/O failed.
    #[error("model broker I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// JSON request or response was malformed.
    #[error("model broker JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    /// A request exceeded the self-test safety bound.
    #[error("model broker request exceeded {MAX_REQUEST_BYTES} bytes")]
    RequestTooLarge,
    /// A request ended without its JSON Lines delimiter.
    #[error("model broker request ended without a newline delimiter")]
    IncompleteRequest,
    /// The broker state machine rejected input.
    #[error("model broker state failed: {0}")]
    State(String),
    /// Shared state was poisoned by a panic.
    #[error("model broker state lock was poisoned")]
    StatePoisoned,
    /// The owned broker thread panicked.
    #[error("model broker thread panicked")]
    ThreadPanicked,
}
