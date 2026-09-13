//! Cluster-description controls retain both options and fenced-broker markers.

use super::*;
use testlab_schema::{
    AdminClusterDescription, BrokerClusterState, DescribeClusterAction, DescribeClusterCommand,
};

const CLUSTER_OPERATION: &str = "admin-describe-cluster-1";

#[test]
fn matching_cluster_identity_and_sorted_brokers_pass() {
    let history = authorization_history(true, Some(1));
    assert!(violations(describe_cluster_action(false, Vec::new()), &history).is_empty());
}

#[test]
fn cluster_description_rejects_identity_mismatch() {
    let history = cluster_history(
        Some("public-cluster"),
        vec![1, 2],
        Vec::new(),
        Some("broker-cluster"),
        vec![1, 2],
        false,
        true,
        Some(1),
    );
    assert_contract(
        &violations(describe_cluster_action(false, Vec::new()), &history),
        "ADMIN-008",
    );
}

#[test]
fn cluster_description_rejects_missing_authorization_or_changed_options() {
    let missing = authorization_history(true, None);
    assert_contract(
        &violations(describe_cluster_action(false, Vec::new()), &missing),
        "ADMIN-008",
    );
    for changed in [
        authorization_history(false, Some(1)),
        cluster_history(
            Some("cluster-a"),
            vec![1, 2],
            Vec::new(),
            Some("cluster-a"),
            vec![1, 2],
            true,
            true,
            Some(1),
        ),
    ] {
        assert_contract(
            &violations(describe_cluster_action(false, Vec::new()), &changed),
            "ADMIN-008",
        );
    }
}

#[test]
fn fenced_broker_is_excluded_then_included_with_exact_marker() {
    let excluded = describe_cluster_action(false, vec![3]);
    let excluded_history = cluster_history(
        Some("cluster-a"),
        vec![1, 2],
        Vec::new(),
        Some("cluster-a"),
        vec![1, 2],
        false,
        true,
        Some(1),
    );
    assert!(violations(excluded.clone(), &excluded_history).is_empty());

    let included = describe_cluster_action(true, vec![3]);
    let included_history = cluster_history(
        Some("cluster-a"),
        vec![1, 2, 3],
        vec![3],
        Some("cluster-a"),
        vec![1, 2],
        true,
        true,
        Some(1),
    );
    assert!(violations(included.clone(), &included_history).is_empty());

    let leaked = cluster_history(
        Some("cluster-a"),
        vec![1, 2, 3],
        vec![3],
        Some("cluster-a"),
        vec![1, 2],
        false,
        true,
        Some(1),
    );
    assert_contract(&violations(excluded, &leaked), "ADMIN-008");

    let missing_marker = cluster_history(
        Some("cluster-a"),
        vec![1, 2, 3],
        Vec::new(),
        Some("cluster-a"),
        vec![1, 2],
        true,
        true,
        Some(1),
    );
    assert_contract(&violations(included, &missing_marker), "ADMIN-008");
}

#[test]
fn admin_commands_must_follow_declared_scenario_order() {
    let topic_operation = operation(TOPIC_OPERATION);
    let cluster_operation = operation(CLUSTER_OPERATION);
    let history = vec![
        command(
            0,
            AdapterCommand::DescribeCluster(DescribeClusterCommand {
                client_id: client(),
                operation_id: cluster_operation.clone(),
                include_fenced_brokers: false,
                include_authorized_operations: true,
                timeout_ms: 1_000,
            }),
        ),
        event(
            1,
            AdapterEvent::ClusterDescribed(AdminClusterDescription {
                operation_id: cluster_operation.clone(),
                cluster_id: Some("cluster-a".to_owned()),
                broker_ids: vec![1, 2],
                fenced_broker_ids: Vec::new(),
                authorized_operations: Some(1),
            }),
        ),
        state(
            2,
            BrokerStateObservation::Cluster(BrokerClusterState {
                observation: 0,
                operation_id: cluster_operation,
                cluster_id: Some("cluster-a".to_owned()),
                broker_ids: vec![1, 2],
            }),
        ),
        command(
            3,
            AdapterCommand::DeleteTopic(DeleteTopicCommand {
                client_id: client(),
                operation_id: topic_operation.clone(),
                topic: "records".to_owned(),
                timeout_ms: 1_000,
            }),
        ),
        event(
            4,
            AdapterEvent::TopicDeleted(AdminTopicCompletion {
                operation_id: topic_operation.clone(),
                topic: "records".to_owned(),
            }),
        ),
        state(
            5,
            BrokerStateObservation::Topic(BrokerTopicState {
                observation: 1,
                operation_id: topic_operation,
                topic: "records".to_owned(),
                exists: false,
                partitions: Vec::new(),
            }),
        ),
    ];
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario
        .steps
        .insert(2, step("delete-topic", delete_topic_action()));
    scenario.steps.insert(
        3,
        step(
            "describe-cluster",
            describe_cluster_action(false, Vec::new()),
        ),
    );
    let index = HistoryIndex::build(&history);
    let mut actual = Vec::new();

    verify_admin(&scenario, &index, &[], &mut actual);

    assert_contract(&actual, "ADMIN-008");
}

fn authorization_history(
    include_authorized_operations: bool,
    authorized_operations: Option<i32>,
) -> Vec<HistoryEntry> {
    cluster_history(
        Some("cluster-a"),
        vec![1, 2],
        Vec::new(),
        Some("cluster-a"),
        vec![1, 2],
        false,
        include_authorized_operations,
        authorized_operations,
    )
}

#[allow(clippy::too_many_arguments)]
fn cluster_history(
    public_cluster: Option<&str>,
    public_brokers: Vec<i32>,
    fenced_brokers: Vec<i32>,
    observed_cluster: Option<&str>,
    observed_brokers: Vec<i32>,
    include_fenced_brokers: bool,
    include_authorized_operations: bool,
    authorized_operations: Option<i32>,
) -> Vec<HistoryEntry> {
    let operation_id = operation(CLUSTER_OPERATION);
    vec![
        command(
            0,
            AdapterCommand::DescribeCluster(DescribeClusterCommand {
                client_id: client(),
                operation_id: operation_id.clone(),
                include_fenced_brokers,
                include_authorized_operations,
                timeout_ms: 1_000,
            }),
        ),
        event(
            1,
            AdapterEvent::ClusterDescribed(AdminClusterDescription {
                operation_id: operation_id.clone(),
                cluster_id: public_cluster.map(str::to_owned),
                broker_ids: public_brokers,
                fenced_broker_ids: fenced_brokers,
                authorized_operations,
            }),
        ),
        state(
            2,
            BrokerStateObservation::Cluster(BrokerClusterState {
                observation: 2,
                operation_id,
                cluster_id: observed_cluster.map(str::to_owned),
                broker_ids: observed_brokers,
            }),
        ),
    ]
}

fn describe_cluster_action(
    include_fenced_brokers: bool,
    expected_fenced_broker_ids: Vec<i32>,
) -> ScenarioAction {
    ScenarioAction::DescribeCluster(DescribeClusterAction {
        client_id: client(),
        operation_id: operation(CLUSTER_OPERATION),
        include_fenced_brokers,
        include_authorized_operations: true,
        expected_fenced_broker_ids,
        timeout_ms: 1_000,
    })
}
