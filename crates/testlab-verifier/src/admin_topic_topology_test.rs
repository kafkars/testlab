//! Topic-topology controls reject dropped and internally inconsistent public facts.

use testlab_schema::{
    AdminTopicDescriptionPage, AdminTopicDescriptionValue, AdminTopicPageCursor,
    TopicDescriptionApi,
};

use super::{partition, partitions, verify_single, verify_values};

#[test]
fn complete_metadata_topology_passes() {
    let operation_id = operation();
    let value = description(&[0, 1]);
    let mut violations = Vec::new();

    verify_values(&operation_id, std::iter::once(&value), 7, &mut violations);

    assert!(violations.is_empty());
}

#[test]
fn complete_paginated_topology_with_eligible_leaders_passes() {
    let operation_id = operation();
    let mut aggregate = partitions(&[0, 1]);
    for detail in &mut aggregate {
        detail.eligible_leader_replicas = Some(vec![1]);
        detail.last_known_eligible_leader_replicas = Some(vec![1]);
    }
    let pages = vec![AdminTopicDescriptionPage {
        partitions: vec![0, 1],
        partition_details: aggregate.clone(),
        next_cursor: None,
    }];
    let mut violations = Vec::new();

    verify_single(
        &operation_id,
        TopicDescriptionApi::DescribeTopicPartitions,
        &[0, 1],
        &aggregate,
        &pages,
        7,
        &mut violations,
    );

    assert!(violations.is_empty());
}

#[test]
fn metadata_topology_rejects_missing_isr_and_api_75_only_fields() {
    let operation_id = operation();
    let mut missing_isr = description(&[0]);
    missing_isr.partitions[0].in_sync_replicas.clear();
    let mut eligible = description(&[0]);
    eligible.partitions[0].eligible_leader_replicas = Some(vec![1]);

    for value in [missing_isr, eligible] {
        let mut violations = Vec::new();
        verify_values(&operation_id, std::iter::once(&value), 7, &mut violations);
        assert_contract(&violations);
    }
}

#[test]
fn paginated_topology_rejects_detached_page_detail() {
    let operation_id = operation();
    let aggregate = partitions(&[0, 1]);
    let pages = vec![AdminTopicDescriptionPage {
        partitions: vec![0],
        partition_details: vec![partition(1)],
        next_cursor: Some(AdminTopicPageCursor {
            topic_name: "orders".to_owned(),
            partition_index: 1,
        }),
    }];
    let mut violations = Vec::new();

    verify_single(
        &operation_id,
        TopicDescriptionApi::DescribeTopicPartitions,
        &[0, 1],
        &aggregate,
        &pages,
        7,
        &mut violations,
    );

    assert_contract(&violations);
}

fn description(partition_ids: &[i32]) -> AdminTopicDescriptionValue {
    AdminTopicDescriptionValue {
        topic_id: Some([1; 16]),
        internal: false,
        authorized_operations: None,
        partitions: partitions(partition_ids),
    }
}

fn operation() -> testlab_schema::OperationId {
    testlab_schema::OperationId::new("topology")
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-097"),
        "{violations:?}"
    );
}
