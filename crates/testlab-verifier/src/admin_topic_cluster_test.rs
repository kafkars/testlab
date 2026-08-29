//! Topic-deletion and cluster-description tests join public results to metadata snapshots.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminClusterDescription, AdminTopicCompletion,
    BrokerClusterState, BrokerStateObservation, BrokerTopicState, DeleteTopicAction,
    DeleteTopicCommand, DescribeClusterAction, DescribeClusterCommand, HistoryEntry,
    HistoryPayload, OperationId, ScenarioAction, TerminalStatus, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

const TOPIC_OPERATION: &str = "admin-delete-topic-1";
const CLUSTER_OPERATION: &str = "admin-describe-cluster-1";

#[test]
fn deleted_topic_with_independent_absence_passes() {
    let history = topic_history(1, false, Vec::new());

    assert!(violations(delete_topic_action(), &history).is_empty());
}

#[test]
fn topic_deletion_rejects_duplicate_public_results_or_present_state() {
    for history in [
        topic_history(2, false, Vec::new()),
        topic_history(1, true, vec![0]),
    ] {
        assert_contract(&violations(delete_topic_action(), &history), "ADMIN-007");
    }
}

#[test]
fn matching_cluster_identity_and_sorted_brokers_pass() {
    let history = cluster_history(Some("cluster-a"), vec![1, 2], Some("cluster-a"), vec![1, 2]);

    assert!(violations(describe_cluster_action(), &history).is_empty());
}

#[test]
fn cluster_description_rejects_identity_mismatch() {
    let history = cluster_history(
        Some("public-cluster"),
        vec![1, 2],
        Some("broker-cluster"),
        vec![1, 2],
    );

    assert_contract(
        &violations(describe_cluster_action(), &history),
        "ADMIN-008",
    );
}

#[test]
fn immediate_state_must_precede_the_next_harness_command() {
    let mut history = topic_history(1, false, Vec::new());
    history.insert(2, command(2, AdapterCommand::Finish));
    let Some(state) = history.last_mut() else {
        panic!("topic history omitted its state observation");
    };
    state.sequence = 3;
    state.observed_unix_ms = 3;

    assert_contract(&violations(delete_topic_action(), &history), "ADMIN-007");
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
                timeout_ms: 1_000,
            }),
        ),
        event(
            1,
            AdapterEvent::ClusterDescribed(AdminClusterDescription {
                operation_id: cluster_operation.clone(),
                cluster_id: Some("cluster-a".to_owned()),
                broker_ids: vec![1, 2],
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
    scenario
        .steps
        .insert(3, step("describe-cluster", describe_cluster_action()));
    let index = HistoryIndex::build(&history);
    let mut actual = Vec::new();

    verify_admin(&scenario, &index, &[], &mut actual);

    assert_contract(&actual, "ADMIN-008");
}

fn topic_history(public_count: usize, exists: bool, partitions: Vec<i32>) -> Vec<HistoryEntry> {
    let operation_id = operation(TOPIC_OPERATION);
    let mut history = vec![command(
        0,
        AdapterCommand::DeleteTopic(DeleteTopicCommand {
            client_id: client(),
            operation_id: operation_id.clone(),
            topic: "records".to_owned(),
            timeout_ms: 1_000,
        }),
    )];
    for sequence in 1..=public_count {
        history.push(event(
            sequence as u64,
            AdapterEvent::TopicDeleted(AdminTopicCompletion {
                operation_id: operation_id.clone(),
                topic: "records".to_owned(),
            }),
        ));
    }
    let sequence = public_count as u64 + 1;
    history.push(state(
        sequence,
        BrokerStateObservation::Topic(BrokerTopicState {
            observation: sequence,
            operation_id,
            topic: "records".to_owned(),
            exists,
            partitions,
        }),
    ));
    history
}

fn cluster_history(
    public_cluster: Option<&str>,
    public_brokers: Vec<i32>,
    observed_cluster: Option<&str>,
    observed_brokers: Vec<i32>,
) -> Vec<HistoryEntry> {
    let operation_id = operation(CLUSTER_OPERATION);
    vec![
        command(
            0,
            AdapterCommand::DescribeCluster(DescribeClusterCommand {
                client_id: client(),
                operation_id: operation_id.clone(),
                timeout_ms: 1_000,
            }),
        ),
        event(
            1,
            AdapterEvent::ClusterDescribed(AdminClusterDescription {
                operation_id: operation_id.clone(),
                cluster_id: public_cluster.map(str::to_owned),
                broker_ids: public_brokers,
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

fn delete_topic_action() -> ScenarioAction {
    ScenarioAction::DeleteTopic(DeleteTopicAction {
        client_id: client(),
        operation_id: operation(TOPIC_OPERATION),
        topic: "records".to_owned(),
        expected_error_code: None,
        timeout_ms: 1_000,
    })
}

fn describe_cluster_action() -> ScenarioAction {
    ScenarioAction::DescribeCluster(DescribeClusterAction {
        client_id: client(),
        operation_id: operation(CLUSTER_OPERATION),
        timeout_ms: 1_000,
    })
}

fn violations(action: ScenarioAction, history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.steps.insert(2, step("admin-operation", action));
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    verify_admin(&scenario, &index, &[], &mut violations);
    violations
}

fn state(sequence: u64, observation: BrokerStateObservation) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation { observation },
    }
}

fn assert_contract(violations: &[testlab_schema::Violation], contract: &str) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract),
        "{violations:?}"
    );
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
