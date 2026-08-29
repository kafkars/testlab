//! Kafkars adapter protocol tests cover public handle lifecycle without a broker.

use std::io::Cursor;

use crate::kafkars_api::{ErrorKind, KafkaError};
use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, AdapterSecurity, ClientId, CommandEnvelope,
    CommandId, RunId, ScenarioId,
};

use super::protocol::{emit_client_failure, run_session};
use crate::AdapterError;

#[test]
fn public_client_and_producer_lifecycle_settles() {
    let client = ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"));
    let commands = vec![
        command(
            "hello",
            AdapterCommand::Hello {
                run_id: RunId::new("run-1").unwrap_or_else(|error| panic!("run id: {error}")),
                scenario_id: ScenarioId::new("producer.real")
                    .unwrap_or_else(|error| panic!("scenario id: {error}")),
                broker_endpoints: vec!["127.0.0.1:1".to_owned(), "127.0.0.1:2".to_owned()],
                security: AdapterSecurity::Plaintext,
            },
        ),
        command(
            "client",
            AdapterCommand::CreateClient {
                client_id: client.clone(),
            },
        ),
        command(
            "shutdown",
            AdapterCommand::ShutdownClient { client_id: client },
        ),
        command("finish", AdapterCommand::Finish),
    ];
    let input = commands
        .iter()
        .map(|command| {
            serde_json::to_string(command).unwrap_or_else(|error| panic!("encode command: {error}"))
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let mut output = Vec::new();

    run_session(Cursor::new(input), &mut output)
        .unwrap_or_else(|error| panic!("run protocol session: {error}"));

    let events = String::from_utf8(output)
        .unwrap_or_else(|error| panic!("decode protocol output: {error}"))
        .lines()
        .map(|line| {
            serde_json::from_str::<AdapterEventEnvelope>(line)
                .unwrap_or_else(|error| panic!("decode event: {error}"))
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        events.last().map(|event| &event.event),
        Some(AdapterEvent::Finished)
    ));
}

#[test]
fn public_client_failure_is_a_correlated_normal_event() {
    let envelope = command("flush", AdapterCommand::Finish);
    let mut output = Vec::new();

    emit_client_failure(
        &mut output,
        envelope,
        AdapterError::Client(KafkaError::new(ErrorKind::Backpressure, "flush contended")),
    )
    .unwrap_or_else(|error| panic!("emit client failure: {error}"));

    let event: AdapterEventEnvelope =
        serde_json::from_slice(&output).unwrap_or_else(|error| panic!("decode event: {error}"));
    assert_eq!(event.command_id.as_str(), "flush");
    assert!(matches!(
        event.event,
        AdapterEvent::CommandFailed { code, diagnostic }
            if code == "backpressure" && diagnostic == "flush contended"
    ));
}

#[test]
fn abort_exits_without_claiming_open_resources_were_settled() {
    let client = ClientId::new("client-open").unwrap_or_else(|error| panic!("client id: {error}"));
    let commands = vec![
        command(
            "hello",
            AdapterCommand::Hello {
                run_id: RunId::new("run-abort").unwrap_or_else(|error| panic!("run id: {error}")),
                scenario_id: ScenarioId::new("producer.failed")
                    .unwrap_or_else(|error| panic!("scenario id: {error}")),
                broker_endpoints: vec!["127.0.0.1:1".to_owned()],
                security: AdapterSecurity::Plaintext,
            },
        ),
        command("client", AdapterCommand::CreateClient { client_id: client }),
        command("abort", AdapterCommand::Abort),
    ];
    let input = commands
        .iter()
        .map(|command| {
            serde_json::to_string(command).unwrap_or_else(|error| panic!("encode command: {error}"))
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let mut output = Vec::new();

    run_session(Cursor::new(input), &mut output)
        .unwrap_or_else(|error| panic!("abort protocol session: {error}"));

    let last = String::from_utf8(output)
        .unwrap_or_else(|error| panic!("decode protocol output: {error}"))
        .lines()
        .last()
        .and_then(|line| serde_json::from_str::<AdapterEventEnvelope>(line).ok());
    assert!(matches!(
        last.map(|event| event.event),
        Some(AdapterEvent::Aborted)
    ));
}

fn command(value: &str, command: AdapterCommand) -> CommandEnvelope {
    let id = CommandId::new(value).unwrap_or_else(|error| panic!("command id: {error}"));
    CommandEnvelope::new(id, command)
}
