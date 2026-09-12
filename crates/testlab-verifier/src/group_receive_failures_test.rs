//! Group-receive failure tests reject policy loss, wrong correlation, and false success.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, CommandEnvelope, CommandId, HistoryEntry,
    HistoryPayload, Scenario, ScenarioAction,
};

use crate::index::HistoryIndex;

#[test]
fn exact_configured_failure_passes() {
    let (scenario, history) = fixture();
    assert!(violations(&scenario, &history).is_empty());
}

#[test]
fn wrong_policy_or_successful_receive_fails() {
    let (scenario, mut history) = fixture();
    let HistoryPayload::HarnessCommand { command } = &mut history[0].payload else {
        panic!("creation command missing");
    };
    let AdapterCommand::CreateGroupConsumer { configuration, .. } = &mut command.command else {
        panic!("group creation missing");
    };
    configuration
        .as_mut()
        .unwrap_or_else(|| panic!("policy missing"))
        .offset_reset = testlab_schema::GroupOffsetReset::Earliest;
    assert!(has_contract(&violations(&scenario, &history), "CONS-017"));

    let (scenario, mut history) = fixture();
    history.push(event(
        3,
        "receive",
        AdapterEvent::GroupReceiveCompleted {
            receive_id: id(testlab_schema::OperationId::new("receive-missing-offset")),
            records: Vec::new(),
            committed: false,
            group_epoch: None,
        },
    ));
    assert!(has_contract(&violations(&scenario, &history), "CONS-017"));
}

fn fixture() -> (Scenario, Vec<HistoryEntry>) {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/classic-group-missing-offset-error.toml"
    ))
    .unwrap_or_else(|error| panic!("missing-offset scenario: {error}"));
    let commands = scenario
        .steps
        .iter()
        .filter_map(|step| match &step.action {
            ScenarioAction::CreateGroupConsumer {
                client_id,
                consumer_id,
                group_id,
                topics,
                protocol,
                configuration,
            } => Some(AdapterCommand::CreateGroupConsumer {
                client_id: client_id.clone(),
                consumer_id: consumer_id.clone(),
                group_id: group_id.clone(),
                topics: topics.clone(),
                protocol: *protocol,
                configuration: configuration.clone(),
            }),
            ScenarioAction::GroupReceive {
                consumer_id,
                method,
                receive_id,
                timeout_ms,
                ..
            } => Some(AdapterCommand::GroupReceive {
                consumer_id: consumer_id.clone(),
                method: *method,
                receive_id: receive_id.clone(),
                timeout_ms: *timeout_ms,
            }),
            _ => None,
        })
        .collect::<Vec<_>>();
    let [creation, receive] = commands.as_slice() else {
        panic!("expected creation and receive");
    };
    (
        scenario,
        vec![
            command(0, "create", creation.clone()),
            command(1, "receive", receive.clone()),
            event(
                2,
                "receive",
                AdapterEvent::CommandFailed {
                    code: testlab_schema::GROUP_MISSING_OFFSET_ERROR_CODE.to_owned(),
                    diagnostic: "group assignment has no committed offset".to_owned(),
                },
            ),
        ],
    )
}

fn command(sequence: u64, command_id: &str, value: AdapterCommand) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::HarnessCommand {
            command: CommandEnvelope::new(id(CommandId::new(command_id)), value),
        },
    }
}

fn event(sequence: u64, command_id: &str, value: AdapterEvent) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::AdapterEvent {
            event: AdapterEventEnvelope::new(id(CommandId::new(command_id)), value),
        },
    }
}

fn violations(scenario: &Scenario, history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    crate::group_receive_failures::verify(scenario, &index, &mut violations);
    violations
}

fn has_contract(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
