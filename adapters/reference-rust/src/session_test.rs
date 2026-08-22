//! Reference adapter protocol and delivery-certainty evidence.

use std::io::Cursor;

use testlab_broker::RunningBroker;
use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, ByteString, ClientId, CommandEnvelope,
    CommandId, OperationId, ProducerId, RecordSpec, RunId, ScenarioId, TerminalStatus,
};

use super::session::run_session;

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}

fn command(id_value: &str, command: AdapterCommand) -> CommandEnvelope {
    CommandEnvelope::new(id(CommandId::new(id_value)), command)
}

fn record() -> RecordSpec {
    RecordSpec {
        topic: "records".to_owned(),
        partition: 0,
        sequence: 1,
        key: None,
        value: Some(ByteString::utf8("value")),
        headers: Vec::new(),
    }
}

#[test]
fn full_session_reports_acknowledgment_and_clean_lifecycle() {
    let broker = RunningBroker::start().unwrap_or_else(|error| panic!("start broker: {error}"));
    let client = id(ClientId::new("client-1"));
    let producer = id(ProducerId::new("producer-1"));
    let commands = vec![
        command(
            "cmd-hello",
            AdapterCommand::Hello {
                run_id: id(RunId::new("run-1")),
                scenario_id: id(ScenarioId::new("producer.round-trip")),
                broker_endpoint: broker.endpoint().to_owned(),
            },
        ),
        command(
            "cmd-client",
            AdapterCommand::CreateClient {
                client_id: client.clone(),
            },
        ),
        command(
            "cmd-ready",
            AdapterCommand::AwaitClientReady {
                client_id: client.clone(),
            },
        ),
        command(
            "cmd-producer",
            AdapterCommand::CreateProducer {
                client_id: client.clone(),
                producer_id: producer.clone(),
            },
        ),
        command(
            "cmd-send",
            AdapterCommand::Send {
                producer_id: producer.clone(),
                operation_id: id(OperationId::new("op-1")),
                record: record(),
            },
        ),
        command(
            "cmd-close",
            AdapterCommand::CloseProducer {
                producer_id: producer,
            },
        ),
        command(
            "cmd-shutdown",
            AdapterCommand::ShutdownClient { client_id: client },
        ),
        command("cmd-finish", AdapterCommand::Finish),
    ];
    let input = commands
        .iter()
        .map(|value| {
            serde_json::to_string(value).unwrap_or_else(|error| panic!("encode command: {error}"))
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let mut output = Vec::new();

    run_session(Cursor::new(input.into_bytes()), &mut output)
        .unwrap_or_else(|error| panic!("run adapter: {error}"));

    let events = String::from_utf8(output)
        .unwrap_or_else(|error| panic!("adapter UTF-8: {error}"))
        .lines()
        .map(|line| {
            serde_json::from_str::<AdapterEventEnvelope>(line)
                .unwrap_or_else(|error| panic!("decode event: {error}"))
        })
        .collect::<Vec<_>>();
    assert!(events.iter().any(|event| {
        matches!(
            &event.event,
            AdapterEvent::OperationTerminal {
                status: TerminalStatus::Acknowledged,
                ..
            }
        )
    }));
    assert!(matches!(
        events.last().map(|event| &event.event),
        Some(AdapterEvent::Finished)
    ));
}
