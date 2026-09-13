//! Consumer registration observation tests pin public group, subscription, and rack readback.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, ClientId, CommandEnvelope, CommandId,
    ConsumerId, GroupConsumerRegistrationObservation, GroupProtocol, HistoryEntry, HistoryPayload,
    Scenario, ScenarioAction, ShareConsumerRegistrationObservation, TerminalStatus,
    VisibilityExpectation,
};

use super::verify;
use crate::index::HistoryIndex;
use crate::verify_fixture::{scenario, step};

#[test]
fn registrations_require_exact_public_handle_state() {
    let scenario = registration_scenario();
    let group = group_observation();
    let share = share_observation();
    assert!(violations(&scenario, history(group.clone(), share.clone())).is_empty());

    let mut wrong_group_id = group.clone();
    wrong_group_id.group_id = "other".to_owned();
    assert_contract(
        &violations(&scenario, history(wrong_group_id, share.clone())),
        "CONS-034",
    );

    let mut wrong_group_consumer = group.clone();
    wrong_group_consumer.consumer_id = consumer("other-group-consumer");
    assert_contract(
        &violations(&scenario, history(wrong_group_consumer, share.clone())),
        "CONS-034",
    );

    let mut wrong_group_subscription = group;
    wrong_group_subscription.subscription.reverse();
    assert_contract(
        &violations(&scenario, history(wrong_group_subscription, share.clone())),
        "CONS-034",
    );

    let mut wrong_share_group = share.clone();
    wrong_share_group.group_id = "other".to_owned();
    assert_contract(
        &violations(&scenario, history(group_observation(), wrong_share_group)),
        "SHARE-015",
    );

    let mut wrong_share_consumer = share.clone();
    wrong_share_consumer.consumer_id = consumer("other-share-consumer");
    assert_contract(
        &violations(
            &scenario,
            history(group_observation(), wrong_share_consumer),
        ),
        "SHARE-015",
    );

    let mut wrong_share_subscription = share.clone();
    wrong_share_subscription.subscription.reverse();
    assert_contract(
        &violations(
            &scenario,
            history(group_observation(), wrong_share_subscription),
        ),
        "SHARE-015",
    );

    let mut wrong_rack = share;
    wrong_rack.rack = None;
    assert_contract(
        &violations(&scenario, history(group_observation(), wrong_rack)),
        "SHARE-015",
    );
}

#[test]
fn registration_observations_are_correlated_unique_and_later() {
    let scenario = registration_scenario();
    let mut duplicate_group = history(group_observation(), share_observation());
    duplicate_group.push(event(
        5,
        "group",
        AdapterEvent::GroupConsumerCreated(group_observation()),
    ));
    assert_contract(&violations(&scenario, duplicate_group), "CONS-034");

    let mut duplicate_share = history(group_observation(), share_observation());
    duplicate_share.push(event(
        5,
        "share",
        AdapterEvent::ShareConsumerCreated(share_observation()),
    ));
    assert_contract(&violations(&scenario, duplicate_share), "SHARE-015");

    let early = vec![
        command(1, "group", group_command()),
        event(
            0,
            "group",
            AdapterEvent::GroupConsumerCreated(group_observation()),
        ),
        command(3, "share", share_command()),
        event(
            4,
            "share",
            AdapterEvent::ShareConsumerCreated(share_observation()),
        ),
    ];
    assert_contract(&violations(&scenario, early), "CONS-034");
}

fn registration_scenario() -> Scenario {
    let mut value = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    value.steps = vec![step("group", group_action()), step("share", share_action())];
    value.assertions.clear();
    value
}

fn group_action() -> ScenarioAction {
    ScenarioAction::CreateGroupConsumer {
        client_id: client("client-1"),
        consumer_id: consumer("group-consumer"),
        group_id: "group-1".to_owned(),
        topics: topics(),
        protocol: GroupProtocol::Consumer,
        configuration: None,
    }
}

fn share_action() -> ScenarioAction {
    ScenarioAction::CreateShareConsumer {
        client_id: client("client-1"),
        consumer_id: consumer("share-consumer"),
        group_id: "share-group-1".to_owned(),
        topics: topics(),
        rack: Some("rack-a".to_owned()),
        membership_timeout_ms: 30_000,
        close_timeout_ms: 10_000,
        configuration: None,
    }
}

fn history(
    group: GroupConsumerRegistrationObservation,
    share: ShareConsumerRegistrationObservation,
) -> Vec<HistoryEntry> {
    vec![
        command(1, "group", group_command()),
        event(2, "group", AdapterEvent::GroupConsumerCreated(group)),
        command(3, "share", share_command()),
        event(4, "share", AdapterEvent::ShareConsumerCreated(share)),
    ]
}

fn group_command() -> AdapterCommand {
    match group_action() {
        ScenarioAction::CreateGroupConsumer {
            client_id,
            consumer_id,
            group_id,
            topics,
            protocol,
            configuration,
        } => AdapterCommand::CreateGroupConsumer {
            client_id,
            consumer_id,
            group_id,
            topics,
            protocol,
            configuration,
        },
        _ => unreachable!(),
    }
}

fn share_command() -> AdapterCommand {
    match share_action() {
        ScenarioAction::CreateShareConsumer {
            client_id,
            consumer_id,
            group_id,
            topics,
            rack,
            membership_timeout_ms,
            close_timeout_ms,
            configuration,
        } => AdapterCommand::CreateShareConsumer {
            client_id,
            consumer_id,
            group_id,
            topics,
            rack,
            membership_timeout_ms,
            close_timeout_ms,
            configuration,
        },
        _ => unreachable!(),
    }
}

fn group_observation() -> GroupConsumerRegistrationObservation {
    GroupConsumerRegistrationObservation {
        consumer_id: consumer("group-consumer"),
        group_id: "group-1".to_owned(),
        subscription: topics(),
    }
}

fn share_observation() -> ShareConsumerRegistrationObservation {
    ShareConsumerRegistrationObservation {
        consumer_id: consumer("share-consumer"),
        group_id: "share-group-1".to_owned(),
        subscription: topics(),
        rack: Some("rack-a".to_owned()),
    }
}

fn topics() -> Vec<String> {
    vec!["orders".to_owned(), "returns".to_owned()]
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

fn client(value: &str) -> ClientId {
    ClientId::new(value).unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn consumer(value: &str) -> ConsumerId {
    ConsumerId::new(value).unwrap_or_else(|error| panic!("consumer ID: {error}"))
}

fn command_id(value: &str) -> CommandId {
    CommandId::new(value).unwrap_or_else(|error| panic!("command ID: {error}"))
}

fn violations(scenario: &Scenario, history: Vec<HistoryEntry>) -> Vec<testlab_schema::Violation> {
    let mut violations = Vec::new();
    verify(scenario, &HistoryIndex::build(&history), &mut violations);
    violations
}

fn assert_contract(violations: &[testlab_schema::Violation], contract_id: &str) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract_id),
        "{violations:?}"
    );
}
