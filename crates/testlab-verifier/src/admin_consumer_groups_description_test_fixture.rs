//! Scenario and history fixtures for mixed consumer-group descriptions.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminConsumerGroupsDescription, BrokerConsumerGroupState,
    BrokerStateObservation, ClientId, ConsumedRecord, ConsumerId, DescribeConsumerGroupsAction,
    DescribeConsumerGroupsCommand, GroupMembershipEpoch, GroupProtocol, HistoryEntry,
    HistoryPayload, OperationId, ScenarioAction, TerminalStatus, VisibilityExpectation,
};

use super::values::{classic_outcome, consumer_outcome, expectation};
use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

pub(super) fn history() -> Vec<HistoryEntry> {
    let operation_id = operation("describe-consumer-groups");
    vec![
        receive(
            0,
            "consumer-receive",
            "consumer-topic",
            GroupMembershipEpoch::Consumer { member_epoch: 4 },
        ),
        receive(
            1,
            "classic-receive",
            "classic-topic",
            GroupMembershipEpoch::Classic { generation_id: 3 },
        ),
        command(
            2,
            AdapterCommand::DescribeConsumerGroups(DescribeConsumerGroupsCommand {
                client_id: client(),
                operation_id: operation_id.clone(),
                group_ids: vec!["consumer-group".to_owned(), "classic-group".to_owned()],
                timeout_ms: 2_000,
            }),
        ),
        event(
            3,
            AdapterEvent::ConsumerGroupsDescribed(AdminConsumerGroupsDescription {
                operation_id: operation_id.clone(),
                outcomes: vec![consumer_outcome(), classic_outcome()],
            }),
        ),
        group_state(4, &operation_id, "consumer-group"),
        group_state(5, &operation_id, "classic-group"),
    ]
}

pub(super) fn violations(history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let mut value = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    let actions = [
        create_group(
            "consumer-member",
            "consumer-group",
            "consumer-topic",
            GroupProtocol::Consumer,
        ),
        group_receive("consumer-member", "consumer-receive"),
        create_group(
            "classic-member",
            "classic-group",
            "classic-topic",
            GroupProtocol::Classic,
        ),
        group_receive("classic-member", "classic-receive"),
        describe_action(),
    ];
    for (index, action) in actions.into_iter().enumerate() {
        value
            .steps
            .insert(2 + index, step(&format!("mixed-group-{index}"), action));
    }
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    verify_admin(&value, &index, &[], &mut violations);
    violations
}

fn describe_action() -> ScenarioAction {
    ScenarioAction::DescribeConsumerGroups(DescribeConsumerGroupsAction {
        client_id: client(),
        operation_id: operation("describe-consumer-groups"),
        groups: vec![
            expectation(
                "consumer-group",
                "consumer-topic",
                GroupProtocol::Consumer,
                "uniform",
            ),
            expectation(
                "classic-group",
                "classic-topic",
                GroupProtocol::Classic,
                "range",
            ),
        ],
        timeout_ms: 2_000,
    })
}

fn create_group(
    consumer_id: &str,
    group_id: &str,
    topic: &str,
    protocol: GroupProtocol,
) -> ScenarioAction {
    ScenarioAction::CreateGroupConsumer {
        client_id: client(),
        consumer_id: consumer(consumer_id),
        group_id: group_id.to_owned(),
        topics: vec![topic.to_owned()],
        protocol,
        configuration: None,
    }
}

fn group_receive(consumer_id: &str, receive_id: &str) -> ScenarioAction {
    ScenarioAction::GroupReceive {
        consumer_id: consumer(consumer_id),
        method: Default::default(),
        receive_id: operation(receive_id),
        expected_operation_id: operation("producer-op"),
        processing_acknowledgement_delay_ms: 0,
        timeout_ms: 2_000,
        expected_error_code: None,
    }
}

fn receive(
    sequence: u64,
    receive_id: &str,
    topic: &str,
    group_epoch: GroupMembershipEpoch,
) -> HistoryEntry {
    event(
        sequence,
        AdapterEvent::GroupReceiveCompleted {
            receive_id: operation(receive_id),
            records: vec![ConsumedRecord {
                topic: topic.to_owned(),
                partition: 0,
                offset: 0,
                timestamp_millis: None,
                key: None,
                value: None,
                headers: Vec::new(),
            }],
            committed: true,
            group_epoch: Some(group_epoch),
        },
    )
}

fn group_state(sequence: u64, operation_id: &OperationId, group_id: &str) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::ConsumerGroup(BrokerConsumerGroupState {
                observation: sequence,
                operation_id: operation_id.clone(),
                group_id: group_id.to_owned(),
                exists: true,
                member_count: Some(1),
            }),
        },
    }
}

pub(super) fn description(history: &mut [HistoryEntry]) -> &mut AdminConsumerGroupsDescription {
    let HistoryPayload::AdapterEvent { event } = &mut history[3].payload else {
        panic!("description event fixture");
    };
    let AdapterEvent::ConsumerGroupsDescribed(value) = &mut event.event else {
        panic!("description outcome fixture");
    };
    value
}

pub(super) fn group_observation(entry: &mut HistoryEntry) -> &mut BrokerConsumerGroupState {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entry.payload else {
        panic!("broker-state fixture");
    };
    let BrokerStateObservation::ConsumerGroup(value) = observation else {
        panic!("group-state fixture");
    };
    value
}

pub(super) fn receive_epoch(entry: &mut HistoryEntry, value: GroupMembershipEpoch) {
    let HistoryPayload::AdapterEvent { event } = &mut entry.payload else {
        panic!("receive event fixture");
    };
    let AdapterEvent::GroupReceiveCompleted { group_epoch, .. } = &mut event.event else {
        panic!("group receive fixture");
    };
    *group_epoch = Some(value);
}

pub(super) fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|value| value.contract_id.as_str() == "ADMIN-069"),
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
