//! Kafkars adapter protocol tests cover public handle lifecycle without a broker.

use std::io::Cursor;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, ClientId, CommandEnvelope, CommandId,
    RunId, ScenarioId,
};

use super::protocol::run_session;

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
                broker_endpoint: "127.0.0.1:1".to_owned(),
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

fn command(value: &str, command: AdapterCommand) -> CommandEnvelope {
    let id = CommandId::new(value).unwrap_or_else(|error| panic!("command id: {error}"));
    CommandEnvelope::new(id, command)
}
