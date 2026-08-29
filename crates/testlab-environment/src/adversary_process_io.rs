//! Parent-side readers bound and decode adversary process streams.

use std::io::{BufRead, BufReader, Read};
use std::process::{ChildStderr, ChildStdout};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};

use testlab_schema::AdversaryEvent;

const MAX_LINE_BYTES: usize = 1024 * 1024;
const MAX_STDOUT_BYTES: usize = 16 * 1024 * 1024;
const MAX_STDERR_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub(crate) enum ProcessMessage {
    Event(AdversaryEvent),
    Error(String),
    Eof,
}

#[derive(Debug)]
pub(crate) struct ProcessReaders {
    pub(crate) events: Receiver<ProcessMessage>,
    stdout: JoinHandle<Result<Vec<u8>, String>>,
    stderr: JoinHandle<Result<Vec<u8>, String>>,
}

impl ProcessReaders {
    pub(crate) fn start(stdout: ChildStdout, stderr: ChildStderr) -> Result<Self, String> {
        let (sender, events) = mpsc::channel();
        let stdout = thread::Builder::new()
            .name("testlab-adversary-stdout".to_owned())
            .spawn(move || read_stdout(stdout, &sender))
            .map_err(|error| format!("spawn adversary stdout reader: {error}"))?;
        let stderr = thread::Builder::new()
            .name("testlab-adversary-stderr".to_owned())
            .spawn(move || read_stderr(stderr))
            .map_err(|error| format!("spawn adversary stderr reader: {error}"))?;
        Ok(Self {
            events,
            stdout,
            stderr,
        })
    }

    pub(crate) fn join(self) -> (Result<Vec<u8>, String>, Result<Vec<u8>, String>) {
        let stdout = self
            .stdout
            .join()
            .map_err(|_| "adversary stdout reader panicked".to_owned())
            .and_then(|result| result);
        let stderr = self
            .stderr
            .join()
            .map_err(|_| "adversary stderr reader panicked".to_owned())
            .and_then(|result| result);
        (stdout, stderr)
    }
}

fn read_stdout(stdout: ChildStdout, sender: &Sender<ProcessMessage>) -> Result<Vec<u8>, String> {
    let mut reader = BufReader::new(stdout);
    let mut captured = Vec::new();
    loop {
        let mut line = Vec::new();
        let read = (&mut reader)
            .take((MAX_LINE_BYTES + 1) as u64)
            .read_until(b'\n', &mut line)
            .map_err(|error| format!("read adversary stdout: {error}"))?;
        if read == 0 {
            send(sender, ProcessMessage::Eof);
            return Ok(captured);
        }
        if read > MAX_LINE_BYTES || line.last() != Some(&b'\n') {
            let diagnostic = "adversary stdout line exceeded its bound or was incomplete";
            send(sender, ProcessMessage::Error(diagnostic.to_owned()));
            return Err(diagnostic.to_owned());
        }
        if captured.len().saturating_add(line.len()) > MAX_STDOUT_BYTES {
            let diagnostic = "adversary stdout exceeded its evidence bound";
            send(sender, ProcessMessage::Error(diagnostic.to_owned()));
            return Err(diagnostic.to_owned());
        }
        captured.extend_from_slice(&line);
        match serde_json::from_slice::<AdversaryEvent>(&line) {
            Ok(event) => send(sender, ProcessMessage::Event(event)),
            Err(error) => {
                let diagnostic = format!("adversary emitted malformed JSON: {error}");
                send(sender, ProcessMessage::Error(diagnostic.clone()));
                return Err(diagnostic);
            }
        }
    }
}

fn read_stderr(mut stderr: ChildStderr) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    stderr
        .by_ref()
        .take((MAX_STDERR_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read adversary stderr: {error}"))?;
    if bytes.len() > MAX_STDERR_BYTES {
        return Err("adversary stderr exceeded its evidence bound".to_owned());
    }
    Ok(bytes)
}

fn send(sender: &Sender<ProcessMessage>, message: ProcessMessage) {
    let _ignored = sender.send(message);
}
