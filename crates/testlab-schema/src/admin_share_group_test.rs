//! Share-group Admin schema tests pin versioning, wire separation, and bounded intent.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AdapterCommand, AdapterEvent, AdminShareGroupDescription, AdminShareGroupMember,
    AdminShareGroupOffsetListing, AdminShareGroupTopicAssignment, BrokerShareGroupOffset,
    BrokerShareGroupState, BrokerStateObservation, ClientId, DescribeShareGroupAction,
    DescribeShareGroupCommand, EVIDENCE_SCHEMA_VERSION, ListShareGroupOffsetsAction,
    ListShareGroupOffsetsCommand, OperationId, PROTOCOL_VERSION, SCENARIO_SCHEMA_VERSION, Scenario,
    ScenarioAction,
};

#[test]
fn plural_share_group_offset_cut_advances_all_versioned_boundaries() {
    assert_eq!(PROTOCOL_VERSION, 114);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 117);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 103);
}

#[test]
fn action_command_public_result_and_broker_fact_round_trip() {
    round_trip(&ScenarioAction::DescribeShareGroup(action()));
    round_trip(&AdapterCommand::DescribeShareGroup(command()));
    round_trip(&AdapterEvent::ShareGroupDescribed(description()));
    round_trip(&BrokerStateObservation::ShareGroup(BrokerShareGroupState {
        observation: 7,
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        exists: true,
        state: Some("Stable".to_owned()),
        member_count: Some(1),
    }));
    round_trip(&ScenarioAction::ListShareGroupOffsets(offset_action()));
    round_trip(&AdapterCommand::ListShareGroupOffsets(offset_command()));
    round_trip(&AdapterEvent::ShareGroupOffsetsListed(offset_listing()));
    round_trip(&BrokerStateObservation::ShareGroupOffset(
        BrokerShareGroupOffset {
            observation: 8,
            operation_id: offset_operation(),
            group_id: "share-group-1".to_owned(),
            topic: "share-topic".to_owned(),
            partition: 0,
            start_offset: Some(1),
            lag: Some(1),
        },
    ));
}

#[test]
fn checked_in_share_group_description_scenario_is_valid() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-describe-share-group.toml"
    ))
    .unwrap_or_else(|error| panic!("parse Share-group scenario: {error}"));
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate Share-group scenario: {error}"));
}

#[test]
fn checked_in_share_group_offset_scenario_is_valid() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-list-share-group-offsets.toml"
    ))
    .unwrap_or_else(|error| panic!("parse Share-group offset scenario: {error}"));
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate Share-group offset scenario: {error}"));
}

#[test]
fn validation_rejects_nonstable_or_unbounded_expectations() {
    let clients = BTreeMap::from([(client(), false)]);
    let mut invalid = action();
    invalid.expected_state = "Empty".to_owned();
    invalid.expected_member_count = 0;
    invalid.expected_rack_id = Some(String::new());
    invalid.expected_topic.clear();
    invalid.expected_partition = -1;
    let mut problems = Vec::new();
    crate::admin_action_validation::validate(
        &ScenarioAction::DescribeShareGroup(invalid),
        &clients,
        &mut BTreeSet::new(),
        &mut problems,
    );
    for expected in [
        "expected_state must be Stable",
        "expected_member_count must be between",
        "invalid expected_rack_id",
        "invalid expected_topic",
        "expected_partition must be nonnegative",
    ] {
        assert!(
            problems.iter().any(|problem| problem.contains(expected)),
            "missing {expected:?} in {problems:?}"
        );
    }
}

#[test]
fn validation_rejects_negative_share_group_offset_expectations() {
    let clients = BTreeMap::from([(client(), false)]);
    let mut invalid = offset_action();
    invalid.expected_start_offset = -1;
    invalid.expected_lag = -1;
    let mut problems = Vec::new();
    crate::admin_action_validation::validate(
        &ScenarioAction::ListShareGroupOffsets(invalid),
        &clients,
        &mut BTreeSet::new(),
        &mut problems,
    );
    for expected in [
        "expected_start_offset must be nonnegative",
        "expected_lag must be nonnegative",
    ] {
        assert!(
            problems.iter().any(|problem| problem.contains(expected)),
            "missing {expected:?} in {problems:?}"
        );
    }
}

fn action() -> DescribeShareGroupAction {
    DescribeShareGroupAction {
        client_id: client(),
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        expected_state: "Stable".to_owned(),
        expected_member_count: 1,
        expected_rack_id: Some("rack-a".to_owned()),
        expected_topic: "share-topic".to_owned(),
        expected_partition: 0,
        include_authorized_operations: true,
        timeout_ms: 1_000,
    }
}

fn command() -> DescribeShareGroupCommand {
    DescribeShareGroupCommand {
        client_id: client(),
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        include_authorized_operations: true,
        timeout_ms: 1_000,
    }
}

fn description() -> AdminShareGroupDescription {
    AdminShareGroupDescription {
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        state: "Stable".to_owned(),
        group_epoch: 3,
        assignment_epoch: 4,
        assignor_name: "simple".to_owned(),
        authorized_operations: Some(1),
        members: vec![AdminShareGroupMember {
            member_id: "member-1".to_owned(),
            rack_id: Some("rack-a".to_owned()),
            member_epoch: 5,
            client_id: "client-1".to_owned(),
            subscribed_topics: vec!["share-topic".to_owned()],
            assignments: vec![AdminShareGroupTopicAssignment {
                topic_id: [1; 16],
                topic: "share-topic".to_owned(),
                partitions: vec![0],
            }],
        }],
    }
}

fn offset_action() -> ListShareGroupOffsetsAction {
    ListShareGroupOffsetsAction {
        client_id: client(),
        operation_id: offset_operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        expected_start_offset: 1,
        expected_lag: 1,
        timeout_ms: 1_000,
    }
}

fn offset_command() -> ListShareGroupOffsetsCommand {
    ListShareGroupOffsetsCommand {
        client_id: client(),
        operation_id: offset_operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        timeout_ms: 1_000,
    }
}

fn offset_listing() -> AdminShareGroupOffsetListing {
    AdminShareGroupOffsetListing {
        operation_id: offset_operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        topic_id: [2; 16],
        start_offset: Some(1),
        leader_epoch: Some(0),
        lag: Some(1),
        error_code: None,
    }
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
{
    let encoded = serde_json::to_string(value)
        .unwrap_or_else(|error| panic!("encode Share-group value: {error}"));
    let decoded = serde_json::from_str::<T>(&encoded)
        .unwrap_or_else(|error| panic!("decode Share-group value: {error}"));
    assert_eq!(&decoded, value);
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("describe-share-group").unwrap_or_else(|error| panic!("operation: {error}"))
}

fn offset_operation() -> OperationId {
    OperationId::new("list-share-group-offsets")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
