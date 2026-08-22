//! Terminal stream capture is bounded before bytes become evidence.

use std::io::Read;
use std::thread::{self, JoinHandle};

const MAX_STREAM_BYTES: usize = 16 * 1024 * 1024;

pub(crate) type Reader = JoinHandle<Result<Vec<u8>, String>>;

pub(crate) fn spawn_reader(
    name: &str,
    reader: impl Read + Send + 'static,
) -> Result<Reader, String> {
    thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || read_bounded(reader))
        .map_err(|error| error.to_string())
}

pub(crate) fn join_reader(reader: Reader, name: &str) -> Result<Vec<u8>, String> {
    reader
        .join()
        .map_err(|_| format!("{name} reader panicked"))?
}

fn read_bounded(mut reader: impl Read) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 16 * 1024];
    loop {
        let read = reader.read(&mut chunk).map_err(|error| error.to_string())?;
        if read == 0 {
            return Ok(bytes);
        }
        if bytes.len().saturating_add(read) > MAX_STREAM_BYTES {
            return Err(format!("terminal stream exceeded {MAX_STREAM_BYTES} bytes"));
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
}
