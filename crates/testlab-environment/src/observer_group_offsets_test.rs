//! Plural committed-offset normalization tests pin full snapshots and caller order.

use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use testlab_schema::{BrokerStateObservation, OperationId};

use crate::observer_admin_target::{
    GroupOffsetTarget, GroupOffsetsSelectionTarget, GroupOffsetsTarget,
};
use crate::observer_group_offsets::{normalize_response, snapshot_matches};

#[test]
fn one_query_normalizes_every_partition_in_caller_order_with_consecutive_ordinals() {
    let offsets = offsets(&[("orders-a", 0, Some(5)), ("orders-b", 2, Some(8))]);
    let observed = normalize_response(40, &operation(), &group(), &offsets)
        .unwrap_or_else(|error| panic!("normalize offsets: {error}"));

    assert_eq!(
        facts(&observed),
        [(40, "orders-b", 2, Some(8)), (41, "orders-a", 0, Some(5))]
    );
}

#[test]
fn invalid_committed_offsets_remain_explicit_absence() {
    let offsets = offsets(&[("orders-b", 2, None), ("orders-a", 0, None)]);
    let observed = normalize_response(0, &operation(), &group(), &offsets)
        .unwrap_or_else(|error| panic!("normalize deleted offsets: {error}"));

    assert_eq!(
        facts(&observed),
        [(0, "orders-b", 2, None), (1, "orders-a", 0, None)]
    );
}

#[test]
fn mutation_convergence_requires_one_complete_same_query_snapshot() {
    let targets = target().offsets;
    let first_only = normalize_response(
        0,
        &operation(),
        &group(),
        &offsets(&[("orders-b", 2, Some(8)), ("orders-a", 0, Some(4))]),
    )
    .unwrap_or_else(|error| panic!("normalize first partial snapshot: {error}"));
    let second_only = normalize_response(
        0,
        &operation(),
        &group(),
        &offsets(&[("orders-b", 2, Some(7)), ("orders-a", 0, Some(5))]),
    )
    .unwrap_or_else(|error| panic!("normalize second partial snapshot: {error}"));
    let complete = normalize_response(
        0,
        &operation(),
        &group(),
        &offsets(&[("orders-b", 2, Some(8)), ("orders-a", 0, Some(5))]),
    )
    .unwrap_or_else(|error| panic!("normalize complete snapshot: {error}"));

    assert!(!snapshot_matches(&first_only, &targets));
    assert!(!snapshot_matches(&second_only, &targets));
    assert!(snapshot_matches(&complete, &targets));
}

#[test]
fn missing_extra_or_mismatched_partitions_are_invalid() {
    let missing = offsets(&[("orders-b", 2, Some(8))]);
    assert!(normalize_response(0, &operation(), &group(), &missing).is_err());

    let extra = offsets(&[
        ("orders-b", 2, Some(8)),
        ("orders-a", 0, Some(5)),
        ("audit", 1, Some(2)),
    ]);
    assert!(normalize_response(0, &operation(), &group(), &extra).is_err());

    let mismatched = offsets(&[("orders-b", 2, Some(8)), ("other", 0, Some(5))]);
    assert!(normalize_response(0, &operation(), &group(), &mismatched).is_err());
}

#[test]
fn separate_group_queries_can_continue_one_global_ordinal_sequence() {
    let first_group = normalize_response(
        7,
        &operation(),
        &group(),
        &offsets(&[("orders-b", 2, Some(8)), ("orders-a", 0, Some(5))]),
    )
    .unwrap_or_else(|error| panic!("normalize first group: {error}"));
    let second = GroupOffsetsSelectionTarget {
        group_id: "audit-group".to_owned(),
        offsets: vec![GroupOffsetTarget {
            topic: "audit".to_owned(),
            partition: 1,
            expected_offset: Some(3),
        }],
    };
    let second_group =
        normalize_response(9, &operation(), &second, &offsets(&[("audit", 1, Some(3))]))
            .unwrap_or_else(|error| panic!("normalize second group: {error}"));

    assert_eq!(facts(&first_group)[0].0, 7);
    assert_eq!(facts(&first_group)[1].0, 8);
    assert_eq!(facts(&second_group)[0].0, 9);
}

fn target() -> GroupOffsetsTarget {
    GroupOffsetsTarget {
        operation_id: operation(),
        group_id: "orders-group".to_owned(),
        offsets: group().offsets,
        poll_expected: true,
    }
}

fn group() -> GroupOffsetsSelectionTarget {
    GroupOffsetsSelectionTarget {
        group_id: "orders-group".to_owned(),
        offsets: vec![
            GroupOffsetTarget {
                topic: "orders-b".to_owned(),
                partition: 2,
                expected_offset: Some(8),
            },
            GroupOffsetTarget {
                topic: "orders-a".to_owned(),
                partition: 0,
                expected_offset: Some(5),
            },
        ],
    }
}

fn offsets(values: &[(&str, i32, Option<i64>)]) -> TopicPartitionList {
    let mut offsets = TopicPartitionList::new();
    for (topic, partition, offset) in values {
        offsets
            .add_partition_offset(
                topic,
                *partition,
                offset.map_or(Offset::Invalid, Offset::Offset),
            )
            .unwrap_or_else(|error| panic!("add offset: {error}"));
    }
    offsets
}

fn facts(observations: &[BrokerStateObservation]) -> Vec<(u64, &str, i32, Option<i64>)> {
    observations
        .iter()
        .map(|observation| {
            let BrokerStateObservation::ConsumerGroupOffset(observation) = observation else {
                panic!("consumer group offset state kind");
            };
            (
                observation.observation,
                observation.topic.as_str(),
                observation.partition,
                observation.offset,
            )
        })
        .collect()
}

fn operation() -> OperationId {
    OperationId::new("plural-offsets").unwrap_or_else(|error| panic!("operation ID: {error}"))
}
