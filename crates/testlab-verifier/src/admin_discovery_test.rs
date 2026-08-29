//! Discovery verifier tests require exact public results and independent broker truth.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminOffsetListing, AdminOffsetPosition, AdminTopicDescription,
    AdminTopicsListing, BrokerObservation, BrokerPartitionOffsets, BrokerStateObservation,
    BrokerTopicState, DescribeTopicAction, DescribeTopicCommand, HistoryEntry, HistoryPayload,
    ListOffsetsAction, ListOffsetsCommand, ListTopicsAction, ListTopicsCommand, OperationId,
    ScenarioAction, TerminalStatus, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn exact_description_matches_independent_metadata() {
    let operation_id = operation("describe-1");
    let scenario = admin_scenario(ScenarioAction::DescribeTopic(DescribeTopicAction {
        client_id: client(),
        operation_id: operation_id.clone(),
        topic: "described".to_owned(),
        expected_partitions: Some(vec![0, 1, 2]),
        expected_error_code: None,
        timeout_ms: 1_000,
    }));
    let history = [
        event(
            1,
            AdapterEvent::TopicDescribed(AdminTopicDescription {
                operation_id: operation_id.clone(),
                topic: "described".to_owned(),
                partitions: vec![0, 1, 2],
            }),
        ),
        topic_state(2, operation_id, "described", vec![0, 1, 2]),
    ];

    assert!(admin_violations(&scenario, &history, &[]).is_empty());
}

#[test]
fn description_missing_independent_partition_fails() {
    let operation_id = operation("describe-1");
    let scenario = admin_scenario(ScenarioAction::DescribeTopic(DescribeTopicAction {
        client_id: client(),
        operation_id: operation_id.clone(),
        topic: "described".to_owned(),
        expected_partitions: Some(vec![0, 1, 2]),
        expected_error_code: None,
        timeout_ms: 1_000,
    }));
    let history = [
        event(
            1,
            AdapterEvent::TopicDescribed(AdminTopicDescription {
                operation_id: operation_id.clone(),
                topic: "described".to_owned(),
                partitions: vec![0, 1, 2],
            }),
        ),
        topic_state(2, operation_id, "described", vec![0, 1]),
    ];

    assert_contract(&admin_violations(&scenario, &history, &[]), "ADMIN-003");
}

#[test]
fn topic_list_requires_sorted_public_membership_and_independent_marker() {
    let operation_id = operation("topics-1");
    let scenario = admin_scenario(ScenarioAction::ListTopics(ListTopicsAction {
        client_id: client(),
        operation_id: operation_id.clone(),
        include_internal: false,
        required_topics: vec!["marker".to_owned()],
        timeout_ms: 1_000,
    }));
    let good = [event(
        1,
        AdapterEvent::TopicsListed(AdminTopicsListing {
            operation_id: operation_id.clone(),
            topics: vec!["another".to_owned(), "marker".to_owned()],
        }),
    )];
    let marker = topic_state(2, operation_id.clone(), "marker", vec![0]);
    assert!(admin_violations(&scenario, &[good[0].clone(), marker.clone()], &[]).is_empty());
    assert_contract(&admin_violations(&scenario, &good, &[]), "ADMIN-004");

    let duplicate = [
        event(
            1,
            AdapterEvent::TopicsListed(AdminTopicsListing {
                operation_id: operation_id.clone(),
                topics: vec!["marker".to_owned()],
            }),
        ),
        event(
            2,
            AdapterEvent::TopicsListed(AdminTopicsListing {
                operation_id: operation_id.clone(),
                topics: vec!["marker".to_owned()],
            }),
        ),
        topic_state(3, operation_id.clone(), "marker", vec![0]),
    ];
    assert_contract(&admin_violations(&scenario, &duplicate, &[]), "ADMIN-004");

    let unsorted = [
        event(
            1,
            AdapterEvent::TopicsListed(AdminTopicsListing {
                operation_id: operation_id.clone(),
                topics: vec!["marker".to_owned(), "another".to_owned()],
            }),
        ),
        marker,
    ];
    assert_contract(&admin_violations(&scenario, &unsorted, &[]), "ADMIN-004");
}

#[test]
fn latest_offset_matches_independent_high_watermark() {
    let operation_id = operation("offset-1");
    let scenario = offset_scenario(operation_id.clone(), 2);
    let public = event(
        1,
        AdapterEvent::OffsetListed(AdminOffsetListing {
            operation_id: operation_id.clone(),
            topic: "offsets".to_owned(),
            partition: 0,
            offset: Some(2),
        }),
    );
    let history = [
        public.clone(),
        partition_offsets(2, operation_id.clone(), "offsets", 0, 0, 2),
    ];

    assert!(admin_violations(&scenario, &history, &[]).is_empty());
    let wrong = [
        public,
        partition_offsets(2, operation_id, "other-topic", 0, 0, 2),
    ];
    assert_contract(&admin_violations(&scenario, &wrong, &[]), "ADMIN-005");
}

#[test]
fn latest_offset_rejects_none_or_a_value_beyond_broker_truth() {
    let operation_id = operation("offset-1");
    let scenario = offset_scenario(operation_id.clone(), 2);
    for offset in [None, Some(3)] {
        let history = [
            event(
                1,
                AdapterEvent::OffsetListed(AdminOffsetListing {
                    operation_id: operation_id.clone(),
                    topic: "offsets".to_owned(),
                    partition: 0,
                    offset,
                }),
            ),
            partition_offsets(2, operation_id.clone(), "offsets", 0, 0, 2),
        ];
        assert_contract(&admin_violations(&scenario, &history, &[]), "ADMIN-005");
    }
}

fn offset_scenario(operation_id: OperationId, expected_offset: i64) -> testlab_schema::Scenario {
    admin_scenario(ScenarioAction::ListOffsets(ListOffsetsAction {
        client_id: client(),
        operation_id,
        topic: "offsets".to_owned(),
        partition: 0,
        position: AdminOffsetPosition::Latest,
        expected_offset: Some(expected_offset),
        expected_error_code: None,
        timeout_ms: 1_000,
    }))
}

fn admin_scenario(action: ScenarioAction) -> testlab_schema::Scenario {
    let mut value = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    value.steps.insert(2, step("admin-discovery", action));
    value
}

fn admin_violations(
    scenario: &testlab_schema::Scenario,
    history: &[testlab_schema::HistoryEntry],
    observations: &[BrokerObservation],
) -> Vec<testlab_schema::Violation> {
    let mut issued = vec![command(0, admin_command(&scenario.steps[2].action))];
    issued.extend_from_slice(history);
    let index = HistoryIndex::build(&issued);
    let mut violations = Vec::new();
    verify_admin(scenario, &index, observations, &mut violations);
    violations
}

fn admin_command(action: &ScenarioAction) -> AdapterCommand {
    match action {
        ScenarioAction::DescribeTopic(value) => {
            AdapterCommand::DescribeTopic(DescribeTopicCommand {
                client_id: value.client_id.clone(),
                operation_id: value.operation_id.clone(),
                topic: value.topic.clone(),
                timeout_ms: value.timeout_ms,
            })
        }
        ScenarioAction::ListTopics(value) => AdapterCommand::ListTopics(ListTopicsCommand {
            client_id: value.client_id.clone(),
            operation_id: value.operation_id.clone(),
            include_internal: value.include_internal,
            timeout_ms: value.timeout_ms,
        }),
        ScenarioAction::ListOffsets(value) => AdapterCommand::ListOffsets(ListOffsetsCommand {
            client_id: value.client_id.clone(),
            operation_id: value.operation_id.clone(),
            topic: value.topic.clone(),
            partition: value.partition,
            position: value.position,
            timeout_ms: value.timeout_ms,
        }),
        _ => panic!("fixture action is not an admin discovery operation"),
    }
}

fn topic_state(
    sequence: u64,
    operation_id: OperationId,
    topic: &str,
    partitions: Vec<i32>,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::Topic(BrokerTopicState {
                observation: sequence,
                operation_id,
                topic: topic.to_owned(),
                exists: true,
                partitions,
            }),
        },
    }
}

fn partition_offsets(
    sequence: u64,
    operation_id: OperationId,
    topic: &str,
    partition: i32,
    low_watermark: i64,
    high_watermark: i64,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::PartitionOffsets(BrokerPartitionOffsets {
                observation: sequence,
                operation_id,
                topic: topic.to_owned(),
                partition,
                low_watermark,
                high_watermark,
            }),
        },
    }
}

fn assert_contract(violations: &[testlab_schema::Violation], expected: &str) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == expected),
        "{violations:?}"
    );
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
