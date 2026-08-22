//! Model-broker client preserves certainty across connect, write, and response loss.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::time::Duration;

use testlab_broker::{ModelBrokerRequest, ModelBrokerResponse, ModelBrokerResponseStatus};
use testlab_schema::{OperationId, RecordSpec, TerminalStatus};

const IO_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_RESPONSE_READ: u64 = 1024 * 1024 + 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BrokerTerminal {
    pub(crate) status: TerminalStatus,
    pub(crate) code: Option<String>,
    pub(crate) offset: Option<i64>,
}

pub(crate) fn send(
    endpoint: &str,
    operation_id: OperationId,
    record: RecordSpec,
) -> BrokerTerminal {
    let mut stream = match TcpStream::connect(endpoint) {
        Ok(stream) => stream,
        Err(error) => {
            return definitely_not_sent("connect_failed", &error.to_string());
        }
    };
    if let Err(error) = configure(&stream) {
        return definitely_not_sent("configure_failed", &error.to_string());
    }
    let request = ModelBrokerRequest {
        operation_id,
        record,
    };
    if let Err(error) = write_request(&mut stream, &request) {
        return possibly_sent("request_write_failed", &error.to_string());
    }
    let _ = stream.shutdown(Shutdown::Write);
    read_response(stream)
}

fn configure(stream: &TcpStream) -> Result<(), std::io::Error> {
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    Ok(())
}

fn write_request(
    stream: &mut TcpStream,
    request: &ModelBrokerRequest,
) -> Result<(), BrokerClientError> {
    serde_json::to_writer(&mut *stream, request)?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    Ok(())
}

fn read_response(stream: TcpStream) -> BrokerTerminal {
    let mut reader = BufReader::new(stream).take(MAX_RESPONSE_READ);
    let mut line = String::new();
    let bytes = match reader.read_line(&mut line) {
        Ok(bytes) => bytes,
        Err(error) => {
            return possibly_sent("response_read_failed", &error.to_string());
        }
    };
    if bytes == 0 {
        return possibly_sent("response_lost", "broker closed without a response");
    }
    if bytes > MAX_RESPONSE_BYTES {
        return possibly_sent(
            "response_too_large",
            "model broker response exceeded safety bound",
        );
    }
    if !line.ends_with('\n') {
        return possibly_sent(
            "incomplete_response",
            "model broker response lacked a newline delimiter",
        );
    }
    let response: ModelBrokerResponse = match serde_json::from_str(line.trim_end()) {
        Ok(response) => response,
        Err(error) => return possibly_sent("invalid_response", &error.to_string()),
    };
    normalize(response)
}

fn normalize(response: ModelBrokerResponse) -> BrokerTerminal {
    match response.status {
        ModelBrokerResponseStatus::Acknowledged => BrokerTerminal {
            status: TerminalStatus::Acknowledged,
            code: response.code,
            offset: response.offset,
        },
        ModelBrokerResponseStatus::Rejected => BrokerTerminal {
            status: TerminalStatus::DefinitelyNotSent,
            code: response.code,
            offset: None,
        },
    }
}

fn definitely_not_sent(code: &str, diagnostic: &str) -> BrokerTerminal {
    eprintln!("reference adapter definite failure {code}: {diagnostic}");
    BrokerTerminal {
        status: TerminalStatus::DefinitelyNotSent,
        code: Some(code.to_owned()),
        offset: None,
    }
}

fn possibly_sent(code: &str, diagnostic: &str) -> BrokerTerminal {
    eprintln!("reference adapter uncertain delivery {code}: {diagnostic}");
    BrokerTerminal {
        status: TerminalStatus::PossiblySent,
        code: Some(code.to_owned()),
        offset: None,
    }
}

#[derive(Debug, thiserror::Error)]
enum BrokerClientError {
    #[error("model broker I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("model broker JSON failed: {0}")]
    Json(#[from] serde_json::Error),
}
