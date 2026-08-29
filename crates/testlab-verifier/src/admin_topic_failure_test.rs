//! Duplicate-topic verdicts bind one exact public failure to unchanged broker metadata.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, BrokerStateObservation, BrokerTopicState,
    Capability, ClientId, CommandEnvelope, CommandId, CreateTopicAction, CreateTopicCommand,
    HistoryEntry, HistoryPayload, OperationId, SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction,
    ScenarioId, ScenarioStep, StepId, TOPIC_ALREADY_EXISTS_ERROR_CODE, VerdictStatus,
};

use super::verify;
use crate::verify_fixture::adapter;

const TOPIC: &str = "orders";

#[test]
fn exact_duplicate_failure_with_unchanged_topic_passes() {
    let verdict = verify(&scenario(), &adapter(), &history(expected_failure()), &[]);

    assert!(verdict.is_passed(), "{verdict:?}");
}

#[test]
fn wrong_code_success_or_changed_state_fails_admin_contract() {
    let wrong_code = AdapterEvent::CommandFailed {
        code: "broker:broker_3".to_owned(),
        diagnostic: "unknown topic".to_owned(),
    };
    let success = AdapterEvent::TopicCreated(testlab_schema::AdminTopicCompletion {
        operation_id: operation("duplicate-topic"),
        topic: TOPIC.to_owned(),
    });
    let mut changed = history(expected_failure());
    let Some(HistoryEntry {
        payload: HistoryPayload::BrokerStateObservation { observation },
        ..
    }) = changed.last_mut()
    else {
        panic!("missing duplicate topic observation");
    };
    let BrokerStateObservation::Topic(state) = observation else {
        panic!("wrong observation kind");
    };
    state.partitions.push(2);

    for entries in [history(wrong_code), history(success), changed] {
        assert_admin_failure(&verify(&scenario(), &adapter(), &entries, &[]));
    }
}

#[test]
fn failure_from_another_command_cannot_satisfy_the_duplicate() {
    let mut entries = history(expected_failure());
    let Some(HistoryEntry {
        payload: HistoryPayload::AdapterEvent { event },
        ..
    }) = entries.get_mut(5)
    else {
        panic!("missing duplicate failure");
    };
    event.command_id = command_id("other-command");

    assert_admin_failure(&verify(&scenario(), &adapter(), &entries, &[]));
}

#[test]
fn prerequisite_creation_must_be_issued_before_the_duplicate() {
    let mut entries = history(expected_failure());
    entries.drain(1..4);

    let verdict = verify(&scenario(), &adapter(), &entries, &[]);

    assert_eq!(verdict.status, VerdictStatus::Failed, "{verdict:?}");
    assert!(
        verdict
            .violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-001"),
        "{verdict:?}"
    );
}

fn history(duplicate_result: AdapterEvent) -> Vec<HistoryEntry> {
    vec![
        event(
            0,
            "hello",
            AdapterEvent::Ready {
                descriptor: adapter(),
            },
        ),
        command(1, "create-command", create_command("create-topic")),
        event(
            2,
            "create-command",
            AdapterEvent::TopicCreated(testlab_schema::AdminTopicCompletion {
                operation_id: operation("create-topic"),
                topic: TOPIC.to_owned(),
            }),
        ),
        topic_state(3, "create-topic", vec![0, 1]),
        command(4, "duplicate-command", create_command("duplicate-topic")),
        event(5, "duplicate-command", duplicate_result),
        topic_state(6, "duplicate-topic", vec![0, 1]),
    ]
}

fn scenario() -> Scenario {
    let client_id = client();
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("admin.duplicate-topic-verifier")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "duplicate topic verifier".to_owned(),
        description: "duplicate creation is rejected without mutation".to_owned(),
        timeout_ms: 60_000,
        requires: BTreeSet::from([
            Capability::Admin,
            Capability::Lifecycle,
            Capability::ClientReadiness,
        ]),
        steps: vec![
            step(
                "create-client",
                ScenarioAction::CreateClient {
                    client_id: client_id.clone(),
                },
            ),
            step(
                "await-client",
                ScenarioAction::AwaitClientReady {
                    client_id: client_id.clone(),
                },
            ),
            step("create-topic", create_action("create-topic", None)),
            step(
                "duplicate-topic",
                create_action("duplicate-topic", Some(TOPIC_ALREADY_EXISTS_ERROR_CODE)),
            ),
            step(
                "shutdown-client",
                ScenarioAction::ShutdownClient { client_id },
            ),
        ],
        assertions: Vec::new(),
    }
}

fn create_action(operation_id: &str, expected_error_code: Option<&str>) -> ScenarioAction {
    ScenarioAction::CreateTopic(CreateTopicAction {
        client_id: client(),
        operation_id: operation(operation_id),
        topic: TOPIC.to_owned(),
        partitions: 2,
        replication_factor: 1,
        validate_only: false,
        expected_error_code: expected_error_code.map(str::to_owned),
        timeout_ms: 1_000,
    })
}

fn create_command(operation_id: &str) -> AdapterCommand {
    AdapterCommand::CreateTopic(CreateTopicCommand {
        client_id: client(),
        operation_id: operation(operation_id),
        topic: TOPIC.to_owned(),
        partitions: 2,
        replication_factor: 1,
        validate_only: false,
        timeout_ms: 1_000,
    })
}

fn expected_failure() -> AdapterEvent {
    AdapterEvent::CommandFailed {
        code: TOPIC_ALREADY_EXISTS_ERROR_CODE.to_owned(),
        diagnostic: "topic already exists".to_owned(),
    }
}

fn command(sequence: u64, id: &str, command: AdapterCommand) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::HarnessCommand {
            command: CommandEnvelope::new(command_id(id), command),
        },
    }
}

fn event(sequence: u64, id: &str, event: AdapterEvent) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::AdapterEvent {
            event: AdapterEventEnvelope::new(command_id(id), event),
        },
    }
}

fn topic_state(sequence: u64, operation_id: &str, partitions: Vec<i32>) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::Topic(BrokerTopicState {
                observation: sequence,
                operation_id: operation(operation_id),
                topic: TOPIC.to_owned(),
                exists: true,
                partitions,
            }),
        },
    }
}

fn assert_admin_failure(verdict: &testlab_schema::Verdict) {
    assert_eq!(verdict.status, VerdictStatus::Failed, "{verdict:?}");
    assert!(
        verdict
            .violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-014"),
        "{verdict:?}"
    );
}

fn step(value: &str, action: ScenarioAction) -> ScenarioStep {
    ScenarioStep {
        id: StepId::new(value).unwrap_or_else(|error| panic!("step id: {error}")),
        action,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn command_id(value: &str) -> CommandId {
    CommandId::new(value).unwrap_or_else(|error| panic!("command id: {error}"))
}
