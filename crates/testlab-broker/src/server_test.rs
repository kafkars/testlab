//! Model-broker persistence, loss, and rejection evidence.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;

use testlab_schema::{BrokerBehavior, ByteString, OperationId, RecordSpec};

use super::{ModelBrokerRequest, ModelBrokerResponse, ModelBrokerResponseStatus, RunningBroker};

fn request() -> ModelBrokerRequest {
    ModelBrokerRequest {
        operation_id: OperationId::new("op-1")
            .unwrap_or_else(|error| panic!("fixture id: {error}")),
        record: RecordSpec {
            topic: "records".to_owned(),
            partition: 0,
            sequence: 1,
            key: None,
            value: Some(ByteString::utf8("value")),
            headers: Vec::new(),
        },
    }
}

fn exchange(broker: &RunningBroker) -> Result<Option<ModelBrokerResponse>, String> {
    let mut stream = TcpStream::connect(broker.endpoint()).map_err(|error| error.to_string())?;
    serde_json::to_writer(&mut stream, &request()).map_err(|error| error.to_string())?;
    stream.write_all(b"\n").map_err(|error| error.to_string())?;
    stream.flush().map_err(|error| error.to_string())?;
    let mut line = String::new();
    BufReader::new(stream)
        .read_line(&mut line)
        .map_err(|error| error.to_string())?;
    if line.is_empty() {
        Ok(None)
    } else {
        serde_json::from_str(line.trim_end())
            .map(Some)
            .map_err(|error| error.to_string())
    }
}

#[test]
fn acknowledgment_is_observed_once() {
    let broker = RunningBroker::start().unwrap_or_else(|error| panic!("start broker: {error}"));
    let response = exchange(&broker).unwrap_or_else(|error| panic!("exchange: {error}"));

    assert!(matches!(
        response.map(|value| value.status),
        Some(ModelBrokerResponseStatus::Acknowledged)
    ));
    assert_eq!(
        broker
            .observations()
            .unwrap_or_else(|error| panic!("observe: {error}"))
            .len(),
        1
    );
}

#[test]
fn accepted_request_can_lose_its_response() {
    let broker = RunningBroker::start().unwrap_or_else(|error| panic!("start broker: {error}"));
    broker
        .set_next_behavior(BrokerBehavior::AcceptAndDropResponse)
        .unwrap_or_else(|error| panic!("set behavior: {error}"));

    assert_eq!(exchange(&broker).ok(), Some(None));
    assert_eq!(
        broker
            .observations()
            .unwrap_or_else(|error| panic!("observe: {error}"))
            .len(),
        1
    );
}
