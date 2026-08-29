//! Plural group-admin verifier tests reject reordered, uncorroborated, and baseline-free claims.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminConsumerGroupOffsetOutcome,
    AdminConsumerGroupOffsetsListing, BrokerConsumerGroupOffset, BrokerStateObservation, ClientId,
    ConsumerGroupOffsetExpectation, ConsumerGroupOffsetSelection, HistoryEntry, HistoryPayload,
    ListConsumerGroupOffsetsBatchAction, ListConsumerGroupOffsetsBatchCommand, OperationId,
    ScenarioAction, TerminalStatus, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn exact_ordered_batch_listing_passes() {
    let action = list_action("list-batch", [4, 7]);
    assert!(violations(vec![action], &list_history("list-batch", [4, 7])).is_empty());
}

#[test]
fn reordered_public_or_independent_offsets_fail_admin_023() {
    let action = list_action("list-batch", [4, 7]);
    let mut public_reordered = list_history("list-batch", [4, 7]);
    let HistoryPayload::AdapterEvent { event } = &mut public_reordered[1].payload else {
        panic!("event fixture");
    };
    let AdapterEvent::ConsumerGroupOffsetsListed(value) = &mut event.event else {
        panic!("listing fixture");
    };
    value.outcomes.swap(0, 1);
    assert_contract(
        &violations(vec![action.clone()], &public_reordered),
        "ADMIN-023",
    );

    let mut facts_reordered = list_history("list-batch", [4, 7]);
    facts_reordered.swap(2, 3);
    assert_contract(&violations(vec![action], &facts_reordered), "ADMIN-023");
}

#[test]
fn error_or_wrong_offset_fails_admin_023() {
    let action = list_action("list-batch", [4, 7]);
    let mut history = list_history("list-batch", [4, 7]);
    let HistoryPayload::AdapterEvent { event } = &mut history[1].payload else {
        panic!("event fixture");
    };
    let AdapterEvent::ConsumerGroupOffsetsListed(value) = &mut event.event else {
        panic!("listing fixture");
    };
    value.outcomes[0].error_code = Some("unknown_topic_or_partition".to_owned());
    assert_contract(&violations(vec![action.clone()], &history), "ADMIN-023");

    let mut wrong_state = list_history("list-batch", [4, 7]);
    let HistoryPayload::BrokerStateObservation { observation } = &mut wrong_state[3].payload else {
        panic!("observation fixture");
    };
    let BrokerStateObservation::ConsumerGroupOffset(value) = observation else {
        panic!("offset fixture");
    };
    value.offset = Some(6);
    assert_contract(&violations(vec![action], &wrong_state), "ADMIN-023");
}

#[test]
fn plural_alteration_requires_a_distinct_valid_baseline() {
    use testlab_schema::{
        AdminConsumerGroupOffsetMutationOutcome, AdminConsumerGroupOffsetsMutation,
        AlterConsumerGroupOffsetsAction, AlterConsumerGroupOffsetsCommand,
        ConsumerGroupOffsetAlteration,
    };

    let baseline = list_action("baseline", [4, 7]);
    let mutation = ScenarioAction::AlterConsumerGroupOffsets(AlterConsumerGroupOffsetsAction {
        client_id: client(),
        operation_id: operation("alter"),
        group_id: "group-a".to_owned(),
        offsets: vec![
            ConsumerGroupOffsetAlteration {
                topic: "topic-b".to_owned(),
                partition: 1,
                offset: 0,
            },
            ConsumerGroupOffsetAlteration {
                topic: "topic-a".to_owned(),
                partition: 0,
                offset: 0,
            },
        ],
        timeout_ms: 2_000,
    });
    let mut history = list_history("baseline", [4, 7]);
    history.extend([
        command(
            4,
            AdapterCommand::AlterConsumerGroupOffsets(AlterConsumerGroupOffsetsCommand {
                client_id: client(),
                operation_id: operation("alter"),
                group_id: "group-a".to_owned(),
                offsets: match &mutation {
                    ScenarioAction::AlterConsumerGroupOffsets(value) => value.offsets.clone(),
                    _ => unreachable!(),
                },
                timeout_ms: 2_000,
            }),
        ),
        event(
            5,
            AdapterEvent::ConsumerGroupOffsetsAltered(AdminConsumerGroupOffsetsMutation {
                operation_id: operation("alter"),
                group_id: "group-a".to_owned(),
                outcomes: selections()
                    .into_iter()
                    .map(|value| AdminConsumerGroupOffsetMutationOutcome {
                        topic: value.topic,
                        partition: value.partition,
                        error_code: None,
                    })
                    .collect(),
            }),
        ),
        offset_state(6, "alter", "topic-b", 1, 0),
        offset_state(7, "alter", "topic-a", 0, 0),
    ]);
    assert!(violations(vec![baseline, mutation.clone()], &history).is_empty());
    assert_contract(&violations(vec![mutation], &history[4..]), "ADMIN-025");
}

fn list_action(operation_id: &str, offsets: [i64; 2]) -> ScenarioAction {
    ScenarioAction::ListConsumerGroupOffsetsBatch(ListConsumerGroupOffsetsBatchAction {
        client_id: client(),
        operation_id: operation(operation_id),
        group_id: "group-a".to_owned(),
        require_stable: true,
        partitions: vec![
            expectation("topic-b", 1, offsets[0]),
            expectation("topic-a", 0, offsets[1]),
        ],
        timeout_ms: 2_000,
    })
}

fn list_history(operation_id: &str, offsets: [i64; 2]) -> Vec<HistoryEntry> {
    vec![
        command(
            0,
            AdapterCommand::ListConsumerGroupOffsetsBatch(ListConsumerGroupOffsetsBatchCommand {
                client_id: client(),
                operation_id: operation(operation_id),
                group_id: "group-a".to_owned(),
                require_stable: true,
                partitions: selections(),
                timeout_ms: 2_000,
            }),
        ),
        event(
            1,
            AdapterEvent::ConsumerGroupOffsetsListed(AdminConsumerGroupOffsetsListing {
                operation_id: operation(operation_id),
                group_id: "group-a".to_owned(),
                outcomes: vec![
                    outcome("topic-b", 1, offsets[0]),
                    outcome("topic-a", 0, offsets[1]),
                ],
            }),
        ),
        offset_state(2, operation_id, "topic-b", 1, offsets[0]),
        offset_state(3, operation_id, "topic-a", 0, offsets[1]),
    ]
}

fn offset_state(
    sequence: u64,
    operation_id: &str,
    topic: &str,
    partition: i32,
    offset: i64,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::ConsumerGroupOffset(BrokerConsumerGroupOffset {
                observation: sequence,
                operation_id: operation(operation_id),
                group_id: "group-a".to_owned(),
                topic: topic.to_owned(),
                partition,
                offset: Some(offset),
            }),
        },
    }
}

fn violations(
    actions: Vec<ScenarioAction>,
    history: &[HistoryEntry],
) -> Vec<testlab_schema::Violation> {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    for (index, action) in actions.into_iter().enumerate() {
        scenario
            .steps
            .insert(2 + index, step(&format!("admin-{index}"), action));
    }
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    verify_admin(&scenario, &index, &[], &mut violations);
    violations
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

fn outcome(topic: &str, partition: i32, offset: i64) -> AdminConsumerGroupOffsetOutcome {
    AdminConsumerGroupOffsetOutcome {
        topic: topic.to_owned(),
        partition,
        offset: Some(offset),
        error_code: None,
    }
}

fn selections() -> Vec<ConsumerGroupOffsetSelection> {
    vec![
        ConsumerGroupOffsetSelection {
            topic: "topic-b".to_owned(),
            partition: 1,
        },
        ConsumerGroupOffsetSelection {
            topic: "topic-a".to_owned(),
            partition: 0,
        },
    ]
}

fn assert_contract(violations: &[testlab_schema::Violation], contract: &str) {
    assert!(
        violations
            .iter()
            .any(|value| value.contract_id.as_str() == contract),
        "{violations:?}"
    );
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
