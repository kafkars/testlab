//! Mixed consumer-group description contracts keep expectations off the wire.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    AdapterCommand, AdapterEvent, AdminConsumerGroupDescriptionOutcome,
    AdminConsumerGroupDescriptionValue, AdminConsumerGroupMemberDescription,
    AdminConsumerGroupTopicAssignment, AdminConsumerGroupsDescription, Capability, ClientId,
    ConsumerGroupDescriptionExpectation, ConsumerId, DescribeConsumerGroupsAction,
    DescribeConsumerGroupsCommand, EVIDENCE_SCHEMA_VERSION, GroupProtocol, OperationId,
    PROTOCOL_VERSION, SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction, ScenarioId, ScenarioStep,
    StepId,
};

#[test]
fn versions_cover_mixed_description_protocol_and_evidence() {
    assert_eq!(PROTOCOL_VERSION, 78);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 81);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 67);
}

#[test]
fn expectations_stay_off_wire_and_detailed_outcomes_round_trip() {
    let groups = expectations();
    let action = ScenarioAction::DescribeConsumerGroups(DescribeConsumerGroupsAction {
        client_id: client(),
        operation_id: operation("describe-groups"),
        groups: groups.clone(),
        timeout_ms: 2_000,
    });
    let command = AdapterCommand::DescribeConsumerGroups(DescribeConsumerGroupsCommand {
        client_id: client(),
        operation_id: operation("describe-groups"),
        group_ids: groups.iter().map(|group| group.group_id.clone()).collect(),
        timeout_ms: 2_000,
    });
    let event = AdapterEvent::ConsumerGroupsDescribed(AdminConsumerGroupsDescription {
        operation_id: operation("describe-groups"),
        outcomes: vec![consumer_outcome(), classic_outcome()],
    });

    let action_encoded = encode(&action);
    let command_encoded = encode(&command);
    let event_encoded = encode(&event);
    assert!(action_encoded.contains("expected_assignor_name = \"uniform\""));
    assert!(!command_encoded.contains("expected_assignor_name"));
    assert!(!command_encoded.contains("expected_member_count"));
    assert_order(&command_encoded, "consumer-group", "classic-group");
    assert_order(&event_encoded, "consumer-group", "classic-group");
    assert_eq!(decode::<ScenarioAction>(&action_encoded), action);
    assert_eq!(decode::<AdapterCommand>(&command_encoded), command);
    assert_eq!(decode::<AdapterEvent>(&event_encoded), event);
}

#[test]
fn action_and_transition_require_exact_live_received_protocol_members() {
    let action = describe_action();
    let clients = BTreeMap::from([(client(), false)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();
    crate::admin_action_validation::validate(&action, &clients, &mut operation_ids, &mut problems);
    assert!(problems.is_empty(), "{problems:?}");

    let scenario = scenario(vec![
        create_group("consumer", "consumer-group", GroupProtocol::Consumer),
        receive("consumer", "consumer-receive"),
        create_group("classic", "classic-group", GroupProtocol::Classic),
        receive("classic", "classic-receive"),
        action,
    ]);
    crate::admin_group::description_batch::transition_validation::validate(
        &scenario,
        &mut problems,
    );
    assert!(problems.is_empty(), "{problems:?}");
}

fn describe_action() -> ScenarioAction {
    ScenarioAction::DescribeConsumerGroups(DescribeConsumerGroupsAction {
        client_id: client(),
        operation_id: operation("describe-groups"),
        groups: expectations(),
        timeout_ms: 2_000,
    })
}

fn expectations() -> Vec<ConsumerGroupDescriptionExpectation> {
    vec![
        expectation("consumer-group", GroupProtocol::Consumer, "uniform"),
        expectation("classic-group", GroupProtocol::Classic, "range"),
    ]
}

fn expectation(
    group_id: &str,
    protocol: GroupProtocol,
    assignor: &str,
) -> ConsumerGroupDescriptionExpectation {
    ConsumerGroupDescriptionExpectation {
        group_id: group_id.to_owned(),
        protocol,
        expected_state: "Stable".to_owned(),
        expected_member_count: 1,
        expected_assignor_name: assignor.to_owned(),
        expected_topic: "records".to_owned(),
        expected_partition: 0,
    }
}

fn consumer_outcome() -> AdminConsumerGroupDescriptionOutcome {
    AdminConsumerGroupDescriptionOutcome {
        group_id: "consumer-group".to_owned(),
        description: Some(description(
            GroupProtocol::Consumer,
            None,
            Some(4),
            Some(4),
            "uniform",
            consumer_member(),
        )),
        error_code: None,
    }
}

fn classic_outcome() -> AdminConsumerGroupDescriptionOutcome {
    AdminConsumerGroupDescriptionOutcome {
        group_id: "classic-group".to_owned(),
        description: Some(description(
            GroupProtocol::Classic,
            Some("consumer"),
            None,
            None,
            "range",
            classic_member(),
        )),
        error_code: None,
    }
}

fn description(
    protocol: GroupProtocol,
    protocol_type: Option<&str>,
    group_epoch: Option<i32>,
    assignment_epoch: Option<i32>,
    assignor_name: &str,
    member: AdminConsumerGroupMemberDescription,
) -> AdminConsumerGroupDescriptionValue {
    AdminConsumerGroupDescriptionValue {
        state: "Stable".to_owned(),
        protocol,
        member_count: 1,
        protocol_type: protocol_type.map(str::to_owned),
        group_epoch,
        assignment_epoch,
        assignor_name: assignor_name.to_owned(),
        members: vec![member],
    }
}

fn consumer_member() -> AdminConsumerGroupMemberDescription {
    member(
        Some(4),
        vec!["records".to_owned()],
        vec![assignment()],
        Vec::new(),
        Vec::new(),
    )
}

fn classic_member() -> AdminConsumerGroupMemberDescription {
    member(None, Vec::new(), Vec::new(), vec![1], vec![2])
}

fn member(
    member_epoch: Option<i32>,
    subscribed_topic_names: Vec<String>,
    assignment: Vec<AdminConsumerGroupTopicAssignment>,
    classic_metadata: Vec<u8>,
    classic_assignment: Vec<u8>,
) -> AdminConsumerGroupMemberDescription {
    AdminConsumerGroupMemberDescription {
        member_id: "member-1".to_owned(),
        group_instance_id: None,
        client_id: "client-1".to_owned(),
        client_host: "/127.0.0.1".to_owned(),
        rack_id: None,
        member_epoch,
        subscribed_topic_names,
        subscribed_topic_regex: None,
        assignment,
        target_assignment: Vec::new(),
        member_type: None,
        classic_metadata,
        classic_assignment,
    }
}

fn assignment() -> AdminConsumerGroupTopicAssignment {
    AdminConsumerGroupTopicAssignment {
        topic_id: [1; 16],
        topic_name: "records".to_owned(),
        partitions: vec![0],
    }
}

fn create_group(id: &str, group_id: &str, protocol: GroupProtocol) -> ScenarioAction {
    ScenarioAction::CreateGroupConsumer {
        client_id: client(),
        consumer_id: consumer(id),
        group_id: group_id.to_owned(),
        topics: vec!["records".to_owned()],
        protocol,
        configuration: None,
    }
}

fn receive(id: &str, receive_id: &str) -> ScenarioAction {
    ScenarioAction::GroupReceive {
        consumer_id: consumer(id),
        receive_id: operation(receive_id),
        expected_operation_id: operation("record"),
        timeout_ms: 1_000,
        expected_error_code: None,
    }
}

fn scenario(actions: Vec<ScenarioAction>) -> Scenario {
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("admin.mixed-group-descriptions")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "mixed group descriptions".to_owned(),
        description: "mixed group descriptions retain live protocol facts".to_owned(),
        timeout_ms: 60_000,
        requires: BTreeSet::from([Capability::Admin]),
        steps: actions
            .into_iter()
            .enumerate()
            .map(|(index, action)| ScenarioStep {
                id: StepId::new(format!("step-{index}"))
                    .unwrap_or_else(|error| panic!("step id: {error}")),
                action,
            })
            .collect(),
        assertions: Vec::new(),
    }
}

fn encode<T: serde::Serialize>(value: &T) -> String {
    toml::to_string(value).unwrap_or_else(|error| panic!("encode: {error}"))
}

fn decode<T: serde::de::DeserializeOwned>(value: &str) -> T {
    toml::from_str(value).unwrap_or_else(|error| panic!("decode: {error}"))
}

fn assert_order(value: &str, first: &str, second: &str) {
    assert!(
        value
            .find(first)
            .is_some_and(|left| { value.find(second).is_some_and(|right| left < right) })
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
