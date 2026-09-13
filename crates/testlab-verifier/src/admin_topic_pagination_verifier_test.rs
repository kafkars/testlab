//! Pagination verifier controls reject missing boundaries and detached cursors.

use testlab_schema::{
    AdapterEvent, AdminTopicDescription, AdminTopicDescriptionPage, AdminTopicPageCursor,
    DescribeTopicAction, OperationId, ScenarioAction, TopicDescriptionApi,
    TopicDescriptionPagination,
};

use super::{admin_scenario, admin_violations, assert_contract, event, topic_state};

#[test]
fn detached_continuation_cursor_fails_admin_description_evidence() {
    let operation_id = operation();
    let mut pages = pages();
    pages[0].next_cursor = Some(AdminTopicPageCursor {
        topic_name: "other-topic".to_owned(),
        partition_index: 2,
    });
    let history = history(operation_id.clone(), pages);

    assert_contract(&admin_violations(&scenario(), &history, &[]), "ADMIN-003");
}

#[test]
fn missing_public_page_fails_even_when_the_aggregate_is_complete() {
    let operation_id = operation();
    let history = history(operation_id, Vec::new());

    assert_contract(&admin_violations(&scenario(), &history, &[]), "ADMIN-003");
}

fn scenario() -> testlab_schema::Scenario {
    admin_scenario(ScenarioAction::DescribeTopic(DescribeTopicAction {
        client_id: super::client(),
        operation_id: operation(),
        topic: "paginated".to_owned(),
        api: TopicDescriptionApi::DescribeTopicPartitions,
        pagination: Some(TopicDescriptionPagination {
            response_partition_limit: 2,
            follow_cursors: true,
        }),
        expected_partitions: Some(vec![0, 1, 2]),
        expected_page_partitions: Some(vec![vec![0, 1], vec![2]]),
        expected_error_code: None,
        timeout_ms: 1_000,
    }))
}

fn history(
    operation_id: OperationId,
    pages: Vec<AdminTopicDescriptionPage>,
) -> [testlab_schema::HistoryEntry; 2] {
    [
        event(
            1,
            AdapterEvent::TopicDescribed(AdminTopicDescription {
                operation_id: operation_id.clone(),
                topic: "paginated".to_owned(),
                partitions: vec![0, 1, 2],
                partition_details: crate::admin_topic::topology::partitions(&[0, 1, 2]),
                pages,
            }),
        ),
        topic_state(2, operation_id, "paginated", vec![0, 1, 2]),
    ]
}

fn pages() -> Vec<AdminTopicDescriptionPage> {
    vec![
        AdminTopicDescriptionPage {
            partitions: vec![0, 1],
            partition_details: crate::admin_topic::topology::partitions(&[0, 1]),
            next_cursor: Some(AdminTopicPageCursor {
                topic_name: "paginated".to_owned(),
                partition_index: 2,
            }),
        },
        AdminTopicDescriptionPage {
            partitions: vec![2],
            partition_details: crate::admin_topic::topology::partitions(&[2]),
            next_cursor: None,
        },
    ]
}

fn operation() -> OperationId {
    OperationId::new("describe-pages").unwrap_or_else(|error| panic!("operation id: {error}"))
}
