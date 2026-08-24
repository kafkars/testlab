//! Verdict tests distinguish untouched group-offset steps from contradictory evidence.

use testlab_schema::{
    AdapterCommand, AdapterEvent, BrokerStateObservation, HistoryEntry, HistoryPayload,
    ListConsumerGroupOffsetsAction, ListConsumerGroupOffsetsCommand, ScenarioAction,
    TerminalStatus, Verdict, VisibilityExpectation,
};

use super::verify;
use crate::verify_fixture::{adapter, command, event, scenario, step};

const OPERATION_ID: &str = "admin-group-offset-1";
const GROUP_ID: &str = "group-1";
const TOPIC: &str = "records";

#[test]
fn verdict_rejects_a_missing_group_offset_command_without_an_earlier_failure() {
    assert_admin_violation(&admin_verdict(Vec::new()));
}

#[test]
fn verdict_skips_an_unissued_group_offset_action_after_a_client_failure() {
    let verdict = admin_verdict(vec![event(
        1,
        AdapterEvent::CommandFailed {
            code: "public_failure".to_owned(),
            diagnostic: "earlier command failed".to_owned(),
        },
    )]);

    assert!(
        verdict
            .violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "CLIENT-001"),
        "{verdict:?}"
    );
    assert!(
        verdict
            .violations
            .iter()
            .all(|violation| violation.contract_id.as_str() != "ADMIN-006"),
        "{verdict:?}"
    );
}

#[test]
fn verdict_rejects_forged_group_offset_evidence_without_a_command() {
    let verdict = admin_verdict(vec![public(1), independent(2, 7)]);

    assert_admin_violation(&verdict);
}

#[test]
fn verdict_rejects_wrong_or_duplicate_same_operation_commands() {
    let wrong = command(
        1,
        AdapterCommand::ListConsumerGroupOffsets(group_command("other-topic")),
    );
    assert_admin_violation(&admin_verdict(vec![wrong]));

    let exact = AdapterCommand::ListConsumerGroupOffsets(group_command(TOPIC));
    let duplicate = vec![command(1, exact.clone()), command(2, exact)];
    assert_admin_violation(&admin_verdict(duplicate));
}

fn admin_verdict(entries: Vec<HistoryEntry>) -> Verdict {
    let descriptor = adapter();
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.steps = vec![step("group-offset", group_action())];
    scenario.assertions.clear();
    let mut history = vec![event(
        0,
        AdapterEvent::Ready {
            descriptor: descriptor.clone(),
        },
    )];
    history.extend(entries);
    history.push(command(90, AdapterCommand::Finish));
    history.push(event(91, AdapterEvent::Finished));
    verify(&scenario, &descriptor, &history, &[])
}

fn group_action() -> ScenarioAction {
    ScenarioAction::ListConsumerGroupOffsets(ListConsumerGroupOffsetsAction {
        client_id: client(),
        operation_id: operation(),
        group_id: GROUP_ID.to_owned(),
        topic: TOPIC.to_owned(),
        partition: 0,
        require_stable: true,
        expected_offset: 1,
        timeout_ms: 1_000,
    })
}

fn group_command(topic: &str) -> ListConsumerGroupOffsetsCommand {
    ListConsumerGroupOffsetsCommand {
        client_id: client(),
        operation_id: operation(),
        group_id: GROUP_ID.to_owned(),
        topic: topic.to_owned(),
        partition: 0,
        require_stable: true,
        timeout_ms: 1_000,
    }
}

fn public(sequence: u64) -> HistoryEntry {
    event(
        sequence,
        AdapterEvent::ConsumerGroupOffsetListed {
            operation_id: operation(),
            group_id: GROUP_ID.to_owned(),
            topic: TOPIC.to_owned(),
            partition: 0,
            offset: Some(1),
        },
    )
}

fn independent(sequence: u64, observation: u64) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::ConsumerGroupOffset {
                observation,
                operation_id: operation(),
                group_id: GROUP_ID.to_owned(),
                topic: TOPIC.to_owned(),
                partition: 0,
                offset: Some(1),
            },
        },
    }
}

fn assert_admin_violation(verdict: &Verdict) {
    let violation = verdict
        .violations
        .iter()
        .find(|violation| violation.contract_id.as_str() == "ADMIN-006")
        .unwrap_or_else(|| panic!("{verdict:?}"));
    assert!(
        violation
            .evidence
            .contains(&format!("scenario:operation:{OPERATION_ID}")),
        "{violation:?}"
    );
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation() -> testlab_schema::OperationId {
    testlab_schema::OperationId::new(OPERATION_ID)
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}
