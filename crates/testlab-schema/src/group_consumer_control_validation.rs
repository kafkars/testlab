//! Group-consumer control validation owns handle and structural input checks.

use std::collections::BTreeSet;

use crate::consumer_action_validation::ConsumerStates;
use crate::{
    AssignedStartPosition, ConsumerId, GroupConsumerControl, GroupConsumerControlAction,
    OperationId, TopicPartitionIdentity,
};

pub(crate) fn validate(
    action: &GroupConsumerControlAction,
    consumers: &mut ConsumerStates,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    let Some(state) =
        crate::consumer_action_validation::open(&action.consumer_id, consumers, problems)
    else {
        return;
    };
    if state.group.is_none() {
        problems.push(format!(
            "consumer {} is not a hosted group consumer",
            action.consumer_id
        ));
    }
    if !operation_ids.insert(action.operation_id.clone()) {
        problems.push(format!("duplicate operation id {}", action.operation_id));
    }
    if !(1..=60_000).contains(&action.timeout_ms) {
        problems.push(format!(
            "group control {} timeout_ms must be between 1 and 60000",
            action.operation_id
        ));
    }
    match &action.control {
        GroupConsumerControl::Pause { partitions }
        | GroupConsumerControl::Resume { partitions } => {
            validate_partitions(&action.consumer_id, partitions, problems);
        }
        GroupConsumerControl::Seek {
            partition,
            position,
        } => {
            validate_partition(&action.consumer_id, partition, problems);
            if let AssignedStartPosition::Offset { offset } = position
                && *offset < 0
            {
                problems.push(format!(
                    "consumer {} group seek has negative offset {offset}",
                    action.consumer_id
                ));
            }
        }
    }
}

fn validate_partitions(
    consumer_id: &ConsumerId,
    partitions: &[TopicPartitionIdentity],
    problems: &mut Vec<String>,
) {
    if partitions.is_empty() || partitions.len() > 32 {
        problems.push(format!(
            "consumer {consumer_id} group control partitions must contain 1 to 32 entries"
        ));
    }
    let mut unique = BTreeSet::new();
    for partition in partitions {
        validate_partition(consumer_id, partition, problems);
        if !unique.insert((partition.topic.as_str(), partition.partition)) {
            problems.push(format!(
                "consumer {consumer_id} group control repeats partition {}:{}",
                partition.topic, partition.partition
            ));
        }
    }
}

fn validate_partition(
    consumer_id: &ConsumerId,
    partition: &TopicPartitionIdentity,
    problems: &mut Vec<String>,
) {
    if partition.topic.is_empty() || partition.topic.len() > 249 {
        problems.push(format!(
            "consumer {consumer_id} group control has invalid topic"
        ));
    }
    if partition.partition < 0 {
        problems.push(format!(
            "consumer {consumer_id} group control has negative partition {}",
            partition.partition
        ));
    }
}
