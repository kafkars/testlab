//! Share-group verifier tests require detailed public state and immediate Kafka CLI state.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminShareGroupDescription, AdminShareGroupMember,
    AdminShareGroupTopicAssignment, BrokerShareGroupState, BrokerStateObservation, Capability,
    ClientId, DescribeShareGroupAction, DescribeShareGroupCommand, HistoryEntry, HistoryPayload,
    OperationId, SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction, ScenarioId,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, step};

#[test]
fn exact_public_description_and_immediate_cli_state_pass() {
    assert!(violations(&history()).is_empty());
}

#[test]
fn lossy_public_assignment_fails_share_group_contract() {
    let mut entries = history();
    let HistoryPayload::AdapterEvent { event } = &mut entries[1].payload else {
        panic!("Share-group event history kind");
    };
    let AdapterEvent::ShareGroupDescribed(value) = &mut event.event else {
        panic!("Share-group event kind");
    };
    value.members[0].assignments[0].partitions.clear();
    assert_contract(&violations(&entries));
}

#[test]
fn mismatched_cli_membership_fails_share_group_contract() {
    let mut entries = history();
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[2].payload else {
        panic!("Share-group observation history kind");
    };
    let BrokerStateObservation::ShareGroup(value) = observation else {
        panic!("Share-group observation kind");
    };
    value.member_count = Some(2);
    assert_contract(&violations(&entries));
}

fn history() -> Vec<HistoryEntry> {
    vec![
        command(1, AdapterCommand::DescribeShareGroup(command_value())),
        event(2, AdapterEvent::ShareGroupDescribed(description())),
        HistoryEntry {
            sequence: 3,
            observed_unix_ms: 3,
            payload: HistoryPayload::BrokerStateObservation {
                observation: BrokerStateObservation::ShareGroup(BrokerShareGroupState {
                    observation: 0,
                    operation_id: operation(),
                    group_id: "share-group-1".to_owned(),
                    exists: true,
                    state: Some("Stable".to_owned()),
                    member_count: Some(1),
                }),
            },
        },
    ]
}

fn description() -> AdminShareGroupDescription {
    AdminShareGroupDescription {
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        state: "Stable".to_owned(),
        group_epoch: 3,
        assignment_epoch: 4,
        assignor_name: "simple".to_owned(),
        members: vec![AdminShareGroupMember {
            member_id: "member-1".to_owned(),
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

fn scenario() -> Scenario {
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("kafka.admin-describe-share-group")
            .unwrap_or_else(|error| panic!("scenario: {error}")),
        title: "Share-group description".to_owned(),
        description: "public and independent Share-group state".to_owned(),
        timeout_ms: 10_000,
        requires: BTreeSet::from([Capability::Admin]),
        steps: vec![step(
            "describe-share-group",
            ScenarioAction::DescribeShareGroup(action()),
        )],
        assertions: Vec::new(),
    }
}

fn action() -> DescribeShareGroupAction {
    DescribeShareGroupAction {
        client_id: client(),
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        expected_state: "Stable".to_owned(),
        expected_member_count: 1,
        expected_topic: "share-topic".to_owned(),
        expected_partition: 0,
        timeout_ms: 1_000,
    }
}

fn command_value() -> DescribeShareGroupCommand {
    DescribeShareGroupCommand {
        client_id: client(),
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        timeout_ms: 1_000,
    }
}

fn violations(entries: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(entries);
    let mut violations = Vec::new();
    verify_admin(&scenario(), &index, &[], &mut violations);
    violations
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-037"),
        "{violations:?}"
    );
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("describe-share-group").unwrap_or_else(|error| panic!("operation: {error}"))
}
