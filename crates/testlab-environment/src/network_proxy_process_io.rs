//! Parent-side readers bound and decode network-proxy process streams.

use std::io::{BufRead, BufReader, Read};
use std::process::{ChildStderr, ChildStdout};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};

use testlab_schema::NetworkProxyEvent;

const MAX_LINE_BYTES: usize = 1024 * 1024;
const MAX_STDOUT_BYTES: usize = 16 * 1024 * 1024;
const MAX_STDERR_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub(crate) enum NetworkProcessMessage {
    Event(NetworkProxyEvent),
    Error(String),
    Eof,
}

#[derive(Debug)]
pub(crate) struct NetworkProcessReaders {
    pub(crate) events: Receiver<NetworkProcessMessage>,
    stdout: JoinHandle<Result<Vec<u8>, String>>,
    stderr: JoinHandle<Result<Vec<u8>, String>>,
}

pub(crate) struct NetworkReaderResult {
    pub(crate) messages: Vec<NetworkProcessMessage>,
    pub(crate) stdout: Result<Vec<u8>, String>,
    pub(crate) stderr: Result<Vec<u8>, String>,
}

impl NetworkProcessReaders {
    pub(crate) fn start(stdout: ChildStdout, stderr: ChildStderr) -> Result<Self, String> {
        let (sender, events) = mpsc::channel();
        let stdout = thread::Builder::new()
            .name("testlab-network-proxy-stdout".to_owned())
            .spawn(move || read_stdout(stdout, &sender))
            .map_err(|error| format!("spawn network proxy stdout reader: {error}"))?;
        let stderr = thread::Builder::new()
            .name("testlab-network-proxy-stderr".to_owned())
            .spawn(move || read_stderr(stderr))
            .map_err(|error| format!("spawn network proxy stderr reader: {error}"))?;
        Ok(Self {
            events,
            stdout,
            stderr,
        })
    }

    pub(crate) fn join(self) -> NetworkReaderResult {
        let stdout = self
            .stdout
            .join()
            .map_err(|_| "network proxy stdout reader panicked".to_owned())
            .and_then(|result| result);
        let stderr = self
            .stderr
            .join()
            .map_err(|_| "network proxy stderr reader panicked".to_owned())
            .and_then(|result| result);
        NetworkReaderResult {
            messages: self.events.try_iter().collect(),
            stdout,
            stderr,
        }
    }
}

fn read_stdout(
    stdout: ChildStdout,
    sender: &Sender<NetworkProcessMessage>,
) -> Result<Vec<u8>, String> {
    let mut reader = BufReader::new(stdout);
    let mut captured = Vec::new();
    loop {
        let mut line = Vec::new();
        let read = (&mut reader)
            .take((MAX_LINE_BYTES + 1) as u64)
            .read_until(b'\n', &mut line)
            .map_err(|error| format!("read network proxy stdout: {error}"))?;
        if read == 0 {
            send(sender, NetworkProcessMessage::Eof);
            return Ok(captured);
        }
        if read > MAX_LINE_BYTES || line.last() != Some(&b'\n') {
            return fail(
                sender,
                "network proxy stdout line exceeded its bound or was incomplete",
            );
        }
        if captured.len().saturating_add(line.len()) > MAX_STDOUT_BYTES {
            return fail(sender, "network proxy stdout exceeded its evidence bound");
        }
        captured.extend_from_slice(&line);
        let event = serde_json::from_slice::<NetworkProxyEvent>(&line).map_err(|error| {
            let diagnostic = format!("decode network proxy stdout: {error}");
            send(sender, NetworkProcessMessage::Error(diagnostic.clone()));
            diagnostic
        })?;
        send(sender, NetworkProcessMessage::Event(event));
    }
}

fn read_stderr(stderr: ChildStderr) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    stderr
        .take((MAX_STDERR_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read network proxy stderr: {error}"))?;
    if bytes.len() > MAX_STDERR_BYTES {
        Err("network proxy stderr exceeded its evidence bound".to_owned())
    } else {
        Ok(bytes)
    }
}

fn fail(sender: &Sender<NetworkProcessMessage>, diagnostic: &str) -> Result<Vec<u8>, String> {
    send(sender, NetworkProcessMessage::Error(diagnostic.to_owned()));
    Err(diagnostic.to_owned())
}

fn send(sender: &Sender<NetworkProcessMessage>, message: NetworkProcessMessage) {
    let _ = sender.send(message);
}
