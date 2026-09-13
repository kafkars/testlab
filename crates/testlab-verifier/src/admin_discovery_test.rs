//! Discovery verifier tests require exact public results and independent broker truth.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminOffsetListing, AdminOffsetSelector, AdminTopicDescription,
    AdminTopicDescriptionPage, AdminTopicPageCursor, BrokerObservation, BrokerPartitionOffsets,
    BrokerStateObservation, BrokerTopicState, DescribeTopicAction, DescribeTopicCommand,
    HistoryEntry, HistoryPayload, ListOffsetsAction, ListOffsetsCommand, ListTopicsCommand,
    OperationId, ScenarioAction, TerminalStatus, TopicDescriptionPagination, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[path = "admin_timestamp_offset_verifier_test.rs"]
mod timestamp_tests;
#[path = "admin_topic_listing_test.rs"]
mod topic_listing_tests;
#[path = "admin_topic_pagination_verifier_test.rs"]
mod topic_pagination_tests;

#[test]
fn exact_description_matches_independent_metadata() {
    let operation_id = operation("describe-1");
    let scenario = admin_scenario(ScenarioAction::DescribeTopic(DescribeTopicAction {
        client_id: client(),
        operation_id: operation_id.clone(),
        topic: "described".to_owned(),
        api: testlab_schema::TopicDescriptionApi::DescribeTopicPartitions,
        pagination: Some(TopicDescriptionPagination {
            response_partition_limit: 2,
            follow_cursors: true,
        }),
        expected_partitions: Some(vec![0, 1, 2]),
        expected_page_partitions: Some(vec![vec![0, 1], vec![2]]),
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
                pages: paginated_description_pages(),
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
        api: testlab_schema::TopicDescriptionApi::Metadata,
        pagination: None,
        expected_partitions: Some(vec![0, 1, 2]),
        expected_page_partitions: None,
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
                pages: Vec::new(),
            }),
        ),
        topic_state(2, operation_id, "described", vec![0, 1]),
    ];

    assert_contract(&admin_violations(&scenario, &history, &[]), "ADMIN-003");
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
            timestamp_millis: None,
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
                    timestamp_millis: None,
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
        position: AdminOffsetSelector::Latest,
        read_isolation: Default::default(),
        timestamp_millis: None,
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
                api: value.api,
                pagination: value.pagination,
                timeout_ms: value.timeout_ms,
            })
        }
        ScenarioAction::ListTopics(value) => AdapterCommand::ListTopics(ListTopicsCommand {
            client_id: value.client_id.clone(),
            operation_id: value.operation_id.clone(),
            include_internal: value.include_internal,
            include_authorized_operations: value.include_authorized_operations,
            timeout_ms: value.timeout_ms,
        }),
        ScenarioAction::ListOffsets(value) => AdapterCommand::ListOffsets(ListOffsetsCommand {
            client_id: value.client_id.clone(),
            operation_id: value.operation_id.clone(),
            topic: value.topic.clone(),
            partition: value.partition,
            position: value.position,
            read_isolation: value.read_isolation,
            timestamp_millis: value.timestamp_millis,
            timeout_ms: value.timeout_ms,
        }),
        _ => panic!("fixture action is not an admin discovery operation"),
    }
}

fn paginated_description_pages() -> Vec<AdminTopicDescriptionPage> {
    vec![
        AdminTopicDescriptionPage {
            partitions: vec![0, 1],
            next_cursor: Some(AdminTopicPageCursor {
                topic_name: "described".to_owned(),
                partition_index: 2,
            }),
        },
        AdminTopicDescriptionPage {
            partitions: vec![2],
            next_cursor: None,
        },
    ]
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
