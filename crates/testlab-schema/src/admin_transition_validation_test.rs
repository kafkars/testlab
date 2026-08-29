//! Destructive-admin validation tests require prior, matching state observations.

use std::collections::BTreeSet;

use super::{
    AlterConsumerGroupOffsetAction, Capability, ClientId, DeleteConsumerGroupAction,
    DeleteConsumerGroupOffsetAction, DeleteTopicAction, DescribeConsumerGroupAction,
    DescribeTopicAction, ListConsumerGroupOffsetsAction, OperationId, SCENARIO_SCHEMA_VERSION,
    Scenario, ScenarioAction, ScenarioId, ScenarioStep, StepId,
};

#[test]
fn destructive_admin_actions_reject_missing_preconditions() {
    let scenario = scenario(vec![
        delete_topic("records"),
        alter_offset("group-1", "records", 0, 2),
        delete_offset("group-1", "records", 0),
        delete_group("group-1"),
    ]);

    let problems = validation_problems(&scenario);

    assert_problem(&problems, "requires a prior topic description for records");
    assert_eq!(
        problems
            .iter()
            .filter(|problem| problem.contains("prior committed-offset listing"))
            .count(),
        2
    );
    assert_problem(&problems, "requires a prior zero-member group description");
}

#[test]
fn destructive_admin_actions_accept_matching_preconditions() {
    let scenario = scenario(vec![
        describe_topic("records"),
        delete_topic("records"),
        list_offset("group-1", "records", 0, 1),
        alter_offset("group-1", "records", 0, 2),
        list_offset("group-1", "records", 0, 2),
        delete_offset("group-1", "records", 0),
        describe_group("group-1", 0),
        delete_group("group-1"),
    ]);

    assert!(scenario.validate().is_ok());
}

#[test]
fn offset_alteration_requires_a_different_baseline() {
    let scenario = scenario(vec![
        list_offset("group-1", "records", 0, 2),
        alter_offset("group-1", "records", 0, 2),
    ]);

    let problems = validation_problems(&scenario);

    assert_problem(&problems, "requires a prior different committed offset");
}

#[test]
fn preconditions_must_match_the_destructive_resource() {
    let scenario = scenario(vec![
        describe_topic("other-records"),
        delete_topic("records"),
        list_offset("group-1", "records", 1, 1),
        delete_offset("group-1", "records", 0),
        describe_group("other-group", 0),
        delete_group("group-1"),
    ]);

    let problems = validation_problems(&scenario);

    assert_problem(&problems, "requires a prior topic description for records");
    assert_problem(&problems, "requires a prior committed-offset listing");
    assert_problem(&problems, "requires a prior zero-member group description");
}

fn scenario(actions: Vec<ScenarioAction>) -> Scenario {
    let client_id = client();
    let mut steps = vec![step(
        "create-client",
        ScenarioAction::CreateClient {
            client_id: client_id.clone(),
        },
    )];
    steps.extend(
        actions
            .into_iter()
            .enumerate()
            .map(|(index, action)| step(&format!("admin-{index}"), action)),
    );
    steps.push(step(
        "shutdown-client",
        ScenarioAction::ShutdownClient { client_id },
    ));
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("admin.transition-validation")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "admin transition validation".to_owned(),
        description: "destructive operations require prior observable state".to_owned(),
        timeout_ms: 60_000,
        requires: BTreeSet::from([Capability::Admin, Capability::Lifecycle]),
        steps,
        assertions: Vec::new(),
    }
}

fn describe_topic(topic: &str) -> ScenarioAction {
    ScenarioAction::DescribeTopic(DescribeTopicAction {
        client_id: client(),
        operation_id: operation(&format!("describe-topic-{topic}")),
        topic: topic.to_owned(),
        expected_partitions: Some(vec![0]),
        expected_error_code: None,
        timeout_ms: 1_000,
    })
}

fn delete_topic(topic: &str) -> ScenarioAction {
    ScenarioAction::DeleteTopic(DeleteTopicAction {
        client_id: client(),
        operation_id: operation(&format!("delete-topic-{topic}")),
        topic: topic.to_owned(),
        expected_error_code: None,
        timeout_ms: 1_000,
    })
}

fn list_offset(group_id: &str, topic: &str, partition: i32, offset: i64) -> ScenarioAction {
    ScenarioAction::ListConsumerGroupOffsets(ListConsumerGroupOffsetsAction {
        client_id: client(),
        operation_id: operation(&format!("list-offset-{partition}-{offset}")),
        group_id: group_id.to_owned(),
        topic: topic.to_owned(),
        partition,
        require_stable: true,
        expected_offset: offset,
        timeout_ms: 1_000,
    })
}

fn alter_offset(group_id: &str, topic: &str, partition: i32, offset: i64) -> ScenarioAction {
    ScenarioAction::AlterConsumerGroupOffset(AlterConsumerGroupOffsetAction {
        client_id: client(),
        operation_id: operation(&format!("alter-offset-{partition}-{offset}")),
        group_id: group_id.to_owned(),
        topic: topic.to_owned(),
        partition,
        offset,
        timeout_ms: 1_000,
    })
}

fn delete_offset(group_id: &str, topic: &str, partition: i32) -> ScenarioAction {
    ScenarioAction::DeleteConsumerGroupOffset(DeleteConsumerGroupOffsetAction {
        client_id: client(),
        operation_id: operation(&format!("delete-offset-{partition}")),
        group_id: group_id.to_owned(),
        topic: topic.to_owned(),
        partition,
        timeout_ms: 1_000,
    })
}

fn describe_group(group_id: &str, member_count: u32) -> ScenarioAction {
    ScenarioAction::DescribeConsumerGroup(DescribeConsumerGroupAction {
        client_id: client(),
        operation_id: operation(&format!("describe-group-{group_id}")),
        group_id: group_id.to_owned(),
        expected_member_count: member_count,
        timeout_ms: 1_000,
    })
}

fn delete_group(group_id: &str) -> ScenarioAction {
    ScenarioAction::DeleteConsumerGroup(DeleteConsumerGroupAction {
        client_id: client(),
        operation_id: operation(&format!("delete-group-{group_id}")),
        group_id: group_id.to_owned(),
        timeout_ms: 1_000,
    })
}

fn validation_problems(scenario: &Scenario) -> Vec<String> {
    match scenario.validate() {
        Ok(()) => panic!("scenario must fail validation"),
        Err(error) => error.problems,
    }
}

fn assert_problem(problems: &[String], expected: &str) {
    assert!(
        problems.iter().any(|problem| problem.contains(expected)),
        "missing {expected:?} in {problems:?}"
    );
}

fn step(value: &str, action: ScenarioAction) -> ScenarioStep {
    ScenarioStep {
        id: StepId::new(value).unwrap_or_else(|error| panic!("step id: {error}")),
        action,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
