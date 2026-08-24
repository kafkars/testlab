//! Admin verifier tests distinguish exact topic success from mismatched claims.

use testlab_schema::{
    AdapterEvent, Capability, CreatePartitionsAction, OperationId, ScenarioAction, TerminalStatus,
    VisibilityExpectation,
};

use super::verify;
use crate::verify_fixture::{adapter, event, history, observation, scenario, step};

#[test]
fn exact_admin_topic_completion_passes() {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.requires.insert(Capability::Admin);
    let operation_id = id(OperationId::new("admin-create-1"));
    scenario.steps.insert(
        2,
        step(
            "admin-create",
            ScenarioAction::CreateTopic {
                client_id: id(testlab_schema::ClientId::new("client-1")),
                operation_id: operation_id.clone(),
                topic: "records".to_owned(),
                partitions: 1,
                replication_factor: 1,
                timeout_ms: 1_000,
            },
        ),
    );
    let mut events = history(TerminalStatus::Acknowledged);
    events.push(event(
        10,
        AdapterEvent::TopicCreated {
            operation_id,
            topic: "records".to_owned(),
        },
    ));

    let verdict = verify(&scenario, &adapter(), &events, &[observation(0, "value")]);

    assert!(verdict.is_passed(), "{verdict:?}");
}

#[test]
fn exact_admin_partition_completion_passes() {
    let (scenario, operation_id) = partition_scenario();
    let mut events = history(TerminalStatus::Acknowledged);
    events.push(event(
        10,
        AdapterEvent::TopicPartitionsCreated {
            operation_id,
            topic: "records".to_owned(),
        },
    ));

    let verdict = verify(&scenario, &adapter(), &events, &[observation(0, "value")]);

    assert!(verdict.is_passed(), "{verdict:?}");
}

#[test]
fn missing_admin_partition_completion_fails() {
    let (scenario, _) = partition_scenario();

    let verdict = verify(
        &scenario,
        &adapter(),
        &history(TerminalStatus::Acknowledged),
        &[observation(0, "value")],
    );

    assert!(
        verdict
            .violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-002"),
        "{verdict:?}"
    );
}

#[test]
fn duplicate_admin_partition_completion_fails() {
    let (scenario, operation_id) = partition_scenario();
    let completion = AdapterEvent::TopicPartitionsCreated {
        operation_id,
        topic: "records".to_owned(),
    };
    let mut events = history(TerminalStatus::Acknowledged);
    events.push(event(10, completion.clone()));
    events.push(event(11, completion));

    let verdict = verify(&scenario, &adapter(), &events, &[observation(0, "value")]);

    assert!(
        verdict
            .violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-002"),
        "{verdict:?}"
    );
}

fn partition_scenario() -> (testlab_schema::Scenario, OperationId) {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.requires.insert(Capability::Admin);
    let operation_id = id(OperationId::new("admin-partitions-1"));
    scenario.steps.insert(
        2,
        step(
            "admin-partitions",
            ScenarioAction::CreatePartitions(CreatePartitionsAction {
                client_id: id(testlab_schema::ClientId::new("client-1")),
                operation_id: operation_id.clone(),
                topic: "records".to_owned(),
                total_count: 2,
                timeout_ms: 1_000,
            }),
        ),
    );
    (scenario, operation_id)
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
