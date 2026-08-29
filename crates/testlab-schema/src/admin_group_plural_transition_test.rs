//! Plural group-admin transitions require exact modeled baselines and membership.

use std::collections::BTreeSet;

use super::{
    AlterConsumerGroupOffsetsAction, Capability, ClassicGroupExpectation, ClientId,
    ConsumerGroupOffsetAlteration, ConsumerGroupOffsetExpectation, ConsumerGroupOffsetSelection,
    ConsumerGroupOffsetsExpectation, ConsumerId, DeleteConsumerGroupOffsetsAction,
    DescribeClassicGroupsAction, GroupProtocol, ListConsumerGroupOffsetsBatchAction,
    ListConsumerGroupsOffsetsAction, OperationId, SCENARIO_SCHEMA_VERSION, Scenario,
    ScenarioAction, ScenarioId, ScenarioStep, StepId,
};

#[test]
fn batch_listings_establish_every_exact_plural_mutation_baseline() {
    let scenario = scenario(vec![
        list_batch(
            "list-group-1",
            "group-1",
            vec![expectation("records", 0, 1), expectation("records", 1, 2)],
        ),
        list_groups(
            "list-groups",
            vec![ConsumerGroupOffsetsExpectation {
                group_id: "group-2".to_owned(),
                partitions: vec![expectation("other", 0, 3)],
            }],
        ),
        alter_batch(
            "alter-group-1",
            "group-1",
            vec![alteration("records", 0, 4), alteration("records", 1, 5)],
        ),
        alter_batch("alter-group-2", "group-2", vec![alteration("other", 0, 6)]),
        delete_batch(
            "delete-group-1",
            "group-1",
            vec![selection("records", 0), selection("records", 1)],
        ),
    ]);
    let mut problems = Vec::new();
    crate::admin_group_offset_transition_validation::validate(&scenario, &mut problems);

    assert!(problems.is_empty(), "{problems:?}");
}

#[test]
fn plural_alter_requires_a_different_baseline_for_every_key() {
    let scenario = scenario(vec![
        list_batch("list", "group-1", vec![expectation("records", 0, 1)]),
        alter_batch(
            "alter",
            "group-1",
            vec![alteration("records", 0, 1), alteration("records", 1, 2)],
        ),
    ]);
    let mut problems = Vec::new();

    crate::admin_group_offset_transition_validation::validate(&scenario, &mut problems);

    assert_problem(
        &problems,
        "prior different committed offset for group-1:records:0",
    );
    assert_problem(
        &problems,
        "prior committed-offset listing for group-1:records:1",
    );
}

#[test]
fn plural_mutations_update_and_remove_the_modeled_offsets() {
    let scenario = scenario(vec![
        list_batch("list", "group-1", vec![expectation("records", 0, 1)]),
        alter_batch("alter-2", "group-1", vec![alteration("records", 0, 2)]),
        alter_batch("alter-3", "group-1", vec![alteration("records", 0, 3)]),
        delete_batch("delete-once", "group-1", vec![selection("records", 0)]),
        delete_batch("delete-twice", "group-1", vec![selection("records", 0)]),
    ]);
    let mut problems = Vec::new();

    crate::admin_group_offset_transition_validation::validate(&scenario, &mut problems);

    assert_eq!(problems.len(), 1, "{problems:?}");
    assert_problem(
        &problems,
        "prior committed-offset listing for group-1:records:0",
    );
}

#[test]
fn classic_descriptions_count_only_open_classic_members_with_prior_receives() {
    let scenario = scenario(vec![
        create_group("classic-1", "group-1", GroupProtocol::Classic),
        describe_classic("before-receive", "group-1", 1),
        group_receive("classic-1", "receive-classic"),
        create_group("consumer-1", "group-1", GroupProtocol::Consumer),
        group_receive("consumer-1", "receive-consumer"),
        describe_classic("while-open", "group-1", 1),
        close_group("classic-1"),
        describe_classic("after-close", "group-1", 0),
    ]);
    let mut problems = Vec::new();

    crate::admin_classic_group_transition_validation::validate(&scenario, &mut problems);

    assert_eq!(problems.len(), 1, "{problems:?}");
    assert_problem(
        &problems,
        "models 0 open classic members with prior group_receive",
    );
}

#[test]
fn zero_member_classic_descriptions_still_require_an_established_group() {
    let mut problems = Vec::new();
    crate::admin_classic_group_transition_validation::validate(
        &scenario(vec![describe_classic("never-created", "group-1", 0)]),
        &mut problems,
    );
    assert_problem(
        &problems,
        "requires a prior classic group consumer creation",
    );

    problems.clear();
    crate::admin_classic_group_transition_validation::validate(
        &scenario(vec![
            create_group("classic-1", "group-1", GroupProtocol::Classic),
            close_group("classic-1"),
            describe_classic("after-close", "group-1", 0),
        ]),
        &mut problems,
    );
    assert!(problems.is_empty(), "{problems:?}");
}

fn scenario(actions: Vec<ScenarioAction>) -> Scenario {
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("admin.group-plural-transition")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "plural group transition".to_owned(),
        description: "plural group operations retain exact modeled preconditions".to_owned(),
        timeout_ms: 60_000,
        requires: BTreeSet::from([Capability::Admin]),
        steps: actions
            .into_iter()
            .enumerate()
            .map(|(index, action)| ScenarioStep {
                id: StepId::new(format!("step-{index}"))
                    .unwrap_or_else(|error| panic!("step id: {error}")),
                action,
            })
            .collect(),
        assertions: Vec::new(),
    }
}

fn list_batch(
    operation_id: &str,
    group_id: &str,
    partitions: Vec<ConsumerGroupOffsetExpectation>,
) -> ScenarioAction {
    ScenarioAction::ListConsumerGroupOffsetsBatch(ListConsumerGroupOffsetsBatchAction {
        client_id: client(),
        operation_id: operation(operation_id),
        group_id: group_id.to_owned(),
        require_stable: true,
        partitions,
        timeout_ms: 1_000,
    })
}

fn list_groups(operation_id: &str, groups: Vec<ConsumerGroupOffsetsExpectation>) -> ScenarioAction {
    ScenarioAction::ListConsumerGroupsOffsets(ListConsumerGroupsOffsetsAction {
        client_id: client(),
        operation_id: operation(operation_id),
        require_stable: true,
        groups,
        timeout_ms: 1_000,
    })
}

fn alter_batch(
    operation_id: &str,
    group_id: &str,
    offsets: Vec<ConsumerGroupOffsetAlteration>,
) -> ScenarioAction {
    ScenarioAction::AlterConsumerGroupOffsets(AlterConsumerGroupOffsetsAction {
        client_id: client(),
        operation_id: operation(operation_id),
        group_id: group_id.to_owned(),
        offsets,
        timeout_ms: 1_000,
    })
}

fn delete_batch(
    operation_id: &str,
    group_id: &str,
    partitions: Vec<ConsumerGroupOffsetSelection>,
) -> ScenarioAction {
    ScenarioAction::DeleteConsumerGroupOffsets(DeleteConsumerGroupOffsetsAction {
        client_id: client(),
        operation_id: operation(operation_id),
        group_id: group_id.to_owned(),
        partitions,
        timeout_ms: 1_000,
    })
}

fn create_group(consumer_id: &str, group_id: &str, protocol: GroupProtocol) -> ScenarioAction {
    ScenarioAction::CreateGroupConsumer {
        client_id: client(),
        consumer_id: consumer(consumer_id),
        group_id: group_id.to_owned(),
        topic: "records".to_owned(),
        protocol,
        configuration: None,
    }
}

fn group_receive(consumer_id: &str, receive_id: &str) -> ScenarioAction {
    ScenarioAction::GroupReceive {
        consumer_id: consumer(consumer_id),
        receive_id: operation(receive_id),
        expected_operation_id: operation("producer-op"),
        timeout_ms: 1_000,
        expected_error_code: None,
    }
}

fn close_group(consumer_id: &str) -> ScenarioAction {
    ScenarioAction::CloseGroupConsumer {
        consumer_id: consumer(consumer_id),
    }
}

fn describe_classic(
    operation_id: &str,
    group_id: &str,
    expected_member_count: u32,
) -> ScenarioAction {
    ScenarioAction::DescribeClassicGroups(DescribeClassicGroupsAction {
        client_id: client(),
        operation_id: operation(operation_id),
        groups: vec![ClassicGroupExpectation {
            group_id: group_id.to_owned(),
            expected_member_count,
        }],
        timeout_ms: 1_000,
    })
}

fn expectation(
    topic: &str,
    partition: i32,
    expected_offset: i64,
) -> ConsumerGroupOffsetExpectation {
    ConsumerGroupOffsetExpectation {
        topic: topic.to_owned(),
        partition,
        expected_offset,
    }
}

fn selection(topic: &str, partition: i32) -> ConsumerGroupOffsetSelection {
    ConsumerGroupOffsetSelection {
        topic: topic.to_owned(),
        partition,
    }
}

fn alteration(topic: &str, partition: i32, offset: i64) -> ConsumerGroupOffsetAlteration {
    ConsumerGroupOffsetAlteration {
        topic: topic.to_owned(),
        partition,
        offset,
    }
}

fn assert_problem(problems: &[String], expected: &str) {
    assert!(
        problems.iter().any(|problem| problem.contains(expected)),
        "missing {expected:?} in {problems:?}"
    );
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn consumer(value: &str) -> ConsumerId {
    ConsumerId::new(value).unwrap_or_else(|error| panic!("consumer id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
