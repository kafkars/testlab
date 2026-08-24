//! Discovery verifier tests require exact public results and independent broker truth.

use testlab_schema::{
    AdapterEvent, AdminOffsetPosition, BrokerObservation, ByteString, OperationId, RecordSpec,
    ScenarioAction, TerminalStatus, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{event, scenario, step};

#[test]
fn exact_description_with_every_partition_exercised_passes() {
    let operation_id = operation("describe-1");
    let scenario = admin_scenario(ScenarioAction::DescribeTopic {
        client_id: client(),
        operation_id: operation_id.clone(),
        topic: "described".to_owned(),
        expected_partitions: vec![0, 1, 2],
        timeout_ms: 1_000,
    });
    let history = [event(
        1,
        AdapterEvent::TopicDescribed {
            operation_id,
            topic: "described".to_owned(),
            partitions: vec![0, 1, 2],
        },
    )];
    let observations = [
        observed(0, "described", 0, 0),
        observed(1, "described", 1, 0),
        observed(2, "described", 2, 0),
    ];

    assert!(admin_violations(&scenario, &history, &observations).is_empty());
}

#[test]
fn description_missing_independent_partition_fails() {
    let operation_id = operation("describe-1");
    let scenario = admin_scenario(ScenarioAction::DescribeTopic {
        client_id: client(),
        operation_id: operation_id.clone(),
        topic: "described".to_owned(),
        expected_partitions: vec![0, 1, 2],
        timeout_ms: 1_000,
    });
    let history = [event(
        1,
        AdapterEvent::TopicDescribed {
            operation_id,
            topic: "described".to_owned(),
            partitions: vec![0, 1, 2],
        },
    )];
    let observations = [
        observed(0, "described", 0, 0),
        observed(1, "described", 1, 0),
    ];

    assert_contract(
        &admin_violations(&scenario, &history, &observations),
        "ADMIN-003",
    );
}

#[test]
fn topic_list_requires_sorted_public_membership_and_independent_marker() {
    let operation_id = operation("topics-1");
    let scenario = admin_scenario(ScenarioAction::ListTopics {
        client_id: client(),
        operation_id: operation_id.clone(),
        include_internal: false,
        required_topics: vec!["marker".to_owned()],
        timeout_ms: 1_000,
    });
    let good = [event(
        1,
        AdapterEvent::TopicsListed {
            operation_id: operation_id.clone(),
            topics: vec!["another".to_owned(), "marker".to_owned()],
        },
    )];
    let observations = [observed(0, "marker", 0, 0)];
    assert!(admin_violations(&scenario, &good, &observations).is_empty());
    assert_contract(&admin_violations(&scenario, &good, &[]), "ADMIN-004");

    let duplicate = [
        event(
            1,
            AdapterEvent::TopicsListed {
                operation_id: operation_id.clone(),
                topics: vec!["marker".to_owned()],
            },
        ),
        event(
            2,
            AdapterEvent::TopicsListed {
                operation_id: operation_id.clone(),
                topics: vec!["marker".to_owned()],
            },
        ),
    ];
    assert_contract(
        &admin_violations(&scenario, &duplicate, &observations),
        "ADMIN-004",
    );

    let unsorted = [event(
        1,
        AdapterEvent::TopicsListed {
            operation_id,
            topics: vec!["marker".to_owned(), "another".to_owned()],
        },
    )];
    assert_contract(
        &admin_violations(&scenario, &unsorted, &observations),
        "ADMIN-004",
    );
}

#[test]
fn latest_offset_matches_one_past_independent_maximum() {
    let operation_id = operation("offset-1");
    let scenario = offset_scenario(operation_id.clone(), 2);
    let history = [event(
        1,
        AdapterEvent::OffsetListed {
            operation_id: operation_id.clone(),
            topic: "offsets".to_owned(),
            partition: 0,
            offset: Some(2),
        },
    )];
    let observations = [observed(0, "offsets", 0, 0), observed(1, "offsets", 0, 1)];

    assert!(admin_violations(&scenario, &history, &observations).is_empty());
    let wrong_observations = [observed(0, "other-topic", 0, 0)];
    assert_contract(
        &admin_violations(&scenario, &history, &wrong_observations),
        "ADMIN-005",
    );
}

#[test]
fn latest_offset_rejects_none_or_a_value_beyond_broker_truth() {
    let operation_id = operation("offset-1");
    let scenario = offset_scenario(operation_id.clone(), 2);
    let observations = [observed(0, "offsets", 0, 0), observed(1, "offsets", 0, 1)];
    for offset in [None, Some(3)] {
        let history = [event(
            1,
            AdapterEvent::OffsetListed {
                operation_id: operation_id.clone(),
                topic: "offsets".to_owned(),
                partition: 0,
                offset,
            },
        )];
        assert_contract(
            &admin_violations(&scenario, &history, &observations),
            "ADMIN-005",
        );
    }
}

fn offset_scenario(operation_id: OperationId, expected_offset: i64) -> testlab_schema::Scenario {
    admin_scenario(ScenarioAction::ListOffsets {
        client_id: client(),
        operation_id,
        topic: "offsets".to_owned(),
        partition: 0,
        position: AdminOffsetPosition::Latest,
        expected_offset,
        timeout_ms: 1_000,
    })
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
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    verify_admin(scenario, &index, observations, &mut violations);
    violations
}

fn observed(observation: u64, topic: &str, partition: i32, offset: i64) -> BrokerObservation {
    let record = RecordSpec {
        topic: topic.to_owned(),
        partition,
        sequence: observation,
        key: None,
        value: Some(ByteString::utf8("marker")),
        headers: Vec::new(),
    };
    let digest = record
        .digest()
        .unwrap_or_else(|error| panic!("record digest: {error}"));
    BrokerObservation {
        observation,
        offset,
        operation_id: operation(&format!("marker-{observation}")),
        record,
        digest,
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
