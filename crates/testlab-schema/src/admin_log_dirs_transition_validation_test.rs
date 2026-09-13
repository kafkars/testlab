use std::collections::BTreeSet;

use crate::{
    ClientId, CreateTopicAction, DescribeLogDirsAction, DescribeReplicaLogDirsAction, OperationId,
    SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction, ScenarioId, ScenarioStep, StepId,
};

#[test]
fn log_directory_target_matches_a_prior_created_replica_fixture() {
    let mut scenario = fixture();
    assert!(scenario.validate().is_ok());

    let ScenarioAction::DescribeLogDirs(action) = &mut scenario.steps[2].action else {
        panic!("log-directory action");
    };
    action.expected_replica_count = 2;
    let error = scenario.validation_error("mismatched replica count must fail");
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("must equal the prior topic replication factor"))
    );
}

#[test]
fn replica_log_directory_target_uses_the_same_created_fixture_contract() {
    let mut scenario = fixture();
    scenario.steps[2].action =
        ScenarioAction::DescribeReplicaLogDirs(DescribeReplicaLogDirsAction {
            client_id: client(),
            operation_id: operation("describe-replica-log-dirs"),
            topic: "orders".to_owned(),
            partition: 0,
            expected_replica_count: 1,
            timeout_ms: 1_000,
        });
    assert!(scenario.validate().is_ok());

    let ScenarioAction::DescribeReplicaLogDirs(action) = &mut scenario.steps[2].action else {
        panic!("replica log-directory action");
    };
    action.expected_replica_count = 2;
    assert!(scenario.validate().is_err());
}

fn fixture() -> Scenario {
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("kafka.admin-log-dirs")
            .unwrap_or_else(|error| panic!("scenario ID: {error}")),
        title: "log directories".to_owned(),
        description: "log directories".to_owned(),
        timeout_ms: 90_000,
        requires: BTreeSet::from([crate::Capability::Admin, crate::Capability::Lifecycle]),
        steps: vec![
            step(
                "create-client",
                ScenarioAction::CreateClient(crate::CreateClientAction {
                    client_id: client(),
                    expected_cluster_id: None,
                    expected_error_code: None,
                }),
            ),
            step(
                "create-topic",
                ScenarioAction::CreateTopic(CreateTopicAction {
                    client_id: client(),
                    operation_id: operation("create-topic"),
                    topic: "orders".to_owned(),
                    partitions: 1,
                    replication_factor: 1,
                    replica_assignments: None,
                    configs: Vec::new(),
                    validate_only: false,
                    expected_error_code: None,
                    timeout_ms: 1_000,
                }),
            ),
            step(
                "describe-log-dirs",
                ScenarioAction::DescribeLogDirs(DescribeLogDirsAction {
                    client_id: client(),
                    operation_id: operation("describe-log-dirs"),
                    topic: "orders".to_owned(),
                    partition: 0,
                    expected_replica_count: 1,
                    timeout_ms: 1_000,
                }),
            ),
            step(
                "shutdown-client",
                ScenarioAction::ShutdownClient {
                    client_id: client(),
                },
            ),
        ],
        assertions: Vec::new(),
    }
}

fn step(id: &str, action: ScenarioAction) -> ScenarioStep {
    ScenarioStep {
        id: StepId::new(id).unwrap_or_else(|error| panic!("step ID: {error}")),
        action,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}
