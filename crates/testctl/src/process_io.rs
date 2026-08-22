//! Reader threads bound adapter stdout and stderr before handing data to testctl.

use std::io::{BufRead, BufReader, Read};
use std::process::{ChildStderr, ChildStdout};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;

use testlab_schema::AdapterEventEnvelope;

use crate::process::ProcessMessage;
use crate::run_error::RunFailure;

const MAX_PROTOCOL_BYTES: usize = 4 * 1024 * 1024;
const MAX_PROTOCOL_READ: u64 = 4 * 1024 * 1024 + 1;
const MAX_STDERR_BYTES: usize = 64 * 1024;
const MAX_STDERR_READ: u64 = 64 * 1024 + 1;

#[derive(Debug)]
pub(crate) struct ProcessReaders {
    pub(crate) events: Receiver<ProcessMessage>,
    pub(crate) stderr: Receiver<String>,
    threads: Vec<thread::JoinHandle<()>>,
}

impl Drop for ProcessReaders {
    fn drop(&mut self) {
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

pub(crate) fn start(
    stdout: ChildStdout,
    stderr: ChildStderr,
) -> Result<ProcessReaders, RunFailure> {
    let (event_sender, events) = mpsc::channel();
    let stdout_thread = thread::Builder::new()
        .name("testlab-adapter-stdout".to_owned())
        .spawn(move || read_stdout(stdout, &event_sender))
        .map_err(|error| {
            RunFailure::harness(
                "subject_reader_spawn_failed",
                format!("failed to spawn adapter stdout reader: {error}"),
            )
        })?;
    let (stderr_sender, stderr_receiver) = mpsc::channel();
    let stderr_thread = thread::Builder::new()
        .name("testlab-adapter-stderr".to_owned())
        .spawn(move || read_stderr(stderr, &stderr_sender))
        .map_err(|error| {
            RunFailure::harness(
                "subject_reader_spawn_failed",
                format!("failed to spawn adapter stderr reader: {error}"),
            )
        })?;
    Ok(ProcessReaders {
        events,
        stderr: stderr_receiver,
        threads: vec![stdout_thread, stderr_thread],
    })
}

fn read_stdout(stdout: ChildStdout, sender: &Sender<ProcessMessage>) {
    let mut reader = BufReader::new(stdout);
    loop {
        let mut line = Vec::new();
        let result = (&mut reader)
            .take(MAX_PROTOCOL_READ)
            .read_until(b'\n', &mut line);
        let bytes = match result {
            Ok(bytes) => bytes,
            Err(error) => {
                send(
                    sender,
                    ProcessMessage::Error(format!("failed to read adapter stdout: {error}")),
                );
                return;
            }
        };
        if bytes == 0 {
            send(sender, ProcessMessage::Eof);
            return;
        }
        if bytes > MAX_PROTOCOL_BYTES {
            send(
                sender,
                ProcessMessage::Error("adapter event exceeded the 4 MiB protocol bound".to_owned()),
            );
            return;
        }
        if line.last() != Some(&b'\n') {
            send(
                sender,
                ProcessMessage::Error("adapter stdout ended in an incomplete JSON line".to_owned()),
            );
            return;
        }
        let event = match serde_json::from_slice::<AdapterEventEnvelope>(&line) {
            Ok(event) => event,
            Err(error) => {
                send(
                    sender,
                    ProcessMessage::Error(format!("adapter emitted malformed JSON: {error}")),
                );
                return;
            }
        };
        send(sender, ProcessMessage::Event(event));
    }
}

fn read_stderr(stderr: ChildStderr, sender: &Sender<String>) {
    let mut bytes = Vec::new();
    let result = stderr.take(MAX_STDERR_READ).read_to_end(&mut bytes);
    let value = match result {
        Ok(_) if bytes.len() > MAX_STDERR_BYTES => {
            bytes.truncate(MAX_STDERR_BYTES);
            format!("{}\n[stderr truncated]", String::from_utf8_lossy(&bytes))
        }
        Ok(_) => String::from_utf8_lossy(&bytes).into_owned(),
        Err(error) => format!("failed to capture adapter stderr: {error}"),
    };
    let _ = sender.send(value);
}

fn send(sender: &Sender<ProcessMessage>, message: ProcessMessage) {
    let _ = sender.send(message);
}
