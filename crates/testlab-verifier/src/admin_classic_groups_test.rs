//! Classic-group batch tests require ordered public, broker, and epoch evidence.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminClassicGroupDescriptionOutcome,
    AdminClassicGroupsDescription, BrokerConsumerGroupState, BrokerStateObservation,
    ClassicGroupExpectation, ClientId, ConsumerId, DescribeClassicGroupsAction,
    DescribeClassicGroupsCommand, GroupMembershipEpoch, GroupProtocol, HistoryEntry,
    HistoryPayload, OperationId, ScenarioAction, TerminalStatus, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn exact_classic_group_batch_description_passes() {
    assert!(violations(&classic_history()).is_empty());
}

#[test]
fn classic_description_rejects_public_or_independent_reordering() {
    let mut public = classic_history();
    description(&mut public).outcomes.swap(0, 1);
    assert_contract(&violations(&public));

    let mut independent = classic_history();
    independent.swap(4, 5);
    assert_contract(&violations(&independent));
}

#[test]
fn classic_description_rejects_group_error_or_wrong_broker_count() {
    let mut public_error = classic_history();
    description(&mut public_error).outcomes[0].error_code =
        Some("group_authorization_failed".to_owned());
    assert_contract(&violations(&public_error));

    let mut wrong_count = classic_history();
    group_observation(&mut wrong_count[5]).member_count = Some(2);
    assert_contract(&violations(&wrong_count));
}

#[test]
fn classic_description_rejects_nonclassic_or_nonpositive_epoch() {
    for epoch in [
        Some(GroupMembershipEpoch::Consumer { member_epoch: 1 }),
        Some(GroupMembershipEpoch::Classic { generation_id: 0 }),
        None,
    ] {
        let mut history = classic_history();
        receive_epoch(&mut history[0], epoch);
        assert_contract(&violations(&history));
    }
}

fn classic_history() -> Vec<HistoryEntry> {
    let operation_id = operation("describe-classic");
    vec![
        receive(
            0,
            "receive-b",
            GroupMembershipEpoch::Classic { generation_id: 3 },
        ),
        receive(
            1,
            "receive-a",
            GroupMembershipEpoch::Classic { generation_id: 5 },
        ),
        command(
            2,
            AdapterCommand::DescribeClassicGroups(DescribeClassicGroupsCommand {
                client_id: client(),
                operation_id: operation_id.clone(),
                group_ids: vec!["group-b".to_owned(), "group-a".to_owned()],
                timeout_ms: 2_000,
            }),
        ),
        event(
            3,
            AdapterEvent::ClassicGroupsDescribed(AdminClassicGroupsDescription {
                operation_id: operation_id.clone(),
                outcomes: vec![outcome("group-b"), outcome("group-a")],
            }),
        ),
        group_state(4, &operation_id, "group-b", 1),
        group_state(5, &operation_id, "group-a", 1),
    ]
}

fn violations(history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let mut value = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    let actions = [
        create_group("consumer-b", "group-b"),
        group_receive("consumer-b", "receive-b"),
        create_group("consumer-a", "group-a"),
        group_receive("consumer-a", "receive-a"),
        describe_action(),
    ];
    for (index, action) in actions.into_iter().enumerate() {
        value
            .steps
            .insert(2 + index, step(&format!("classic-{index}"), action));
    }
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    verify_admin(&value, &index, &[], &mut violations);
    violations
}

fn describe_action() -> ScenarioAction {
    ScenarioAction::DescribeClassicGroups(DescribeClassicGroupsAction {
        client_id: client(),
        operation_id: operation("describe-classic"),
        groups: vec![expectation("group-b"), expectation("group-a")],
        timeout_ms: 2_000,
    })
}

fn create_group(consumer_id: &str, group_id: &str) -> ScenarioAction {
    ScenarioAction::CreateGroupConsumer {
        client_id: client(),
        consumer_id: consumer(consumer_id),
        group_id: group_id.to_owned(),
        topic: format!("{group_id}-topic"),
        protocol: GroupProtocol::Classic,
        configuration: None,
    }
}

fn group_receive(consumer_id: &str, receive_id: &str) -> ScenarioAction {
    ScenarioAction::GroupReceive {
        consumer_id: consumer(consumer_id),
        receive_id: operation(receive_id),
        expected_operation_id: operation("producer-op"),
        timeout_ms: 2_000,
        expected_error_code: None,
    }
}

fn receive(sequence: u64, receive_id: &str, epoch: GroupMembershipEpoch) -> HistoryEntry {
    event(
        sequence,
        AdapterEvent::GroupReceiveCompleted {
            receive_id: operation(receive_id),
            records: Vec::new(),
            committed: true,
            group_epoch: Some(epoch),
        },
    )
}

fn group_state(
    sequence: u64,
    operation_id: &OperationId,
    group_id: &str,
    member_count: u32,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::ConsumerGroup(BrokerConsumerGroupState {
                observation: sequence,
                operation_id: operation_id.clone(),
                group_id: group_id.to_owned(),
                exists: true,
                member_count: Some(member_count),
            }),
        },
    }
}

fn outcome(group_id: &str) -> AdminClassicGroupDescriptionOutcome {
    AdminClassicGroupDescriptionOutcome {
        group_id: group_id.to_owned(),
        member_count: Some(1),
        error_code: None,
    }
}

fn expectation(group_id: &str) -> ClassicGroupExpectation {
    ClassicGroupExpectation {
        group_id: group_id.to_owned(),
        expected_member_count: 1,
    }
}

fn description(history: &mut [HistoryEntry]) -> &mut AdminClassicGroupsDescription {
    let HistoryPayload::AdapterEvent { event } = &mut history[3].payload else {
        panic!("description event fixture");
    };
    let AdapterEvent::ClassicGroupsDescribed(value) = &mut event.event else {
        panic!("description outcome fixture");
    };
    value
}

fn group_observation(entry: &mut HistoryEntry) -> &mut BrokerConsumerGroupState {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entry.payload else {
        panic!("broker-state fixture");
    };
    let BrokerStateObservation::ConsumerGroup(value) = observation else {
        panic!("group-state fixture");
    };
    value
}

fn receive_epoch(entry: &mut HistoryEntry, value: Option<GroupMembershipEpoch>) {
    let HistoryPayload::AdapterEvent { event } = &mut entry.payload else {
        panic!("receive event fixture");
    };
    let AdapterEvent::GroupReceiveCompleted { group_epoch, .. } = &mut event.event else {
        panic!("group receive fixture");
    };
    *group_epoch = value;
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|value| value.contract_id.as_str() == "ADMIN-027"),
        "{violations:?}"
    );
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn consumer(value: &str) -> ConsumerId {
    ConsumerId::new(value).unwrap_or_else(|error| panic!("consumer id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
