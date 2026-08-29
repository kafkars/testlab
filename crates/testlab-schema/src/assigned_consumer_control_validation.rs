//! Direct-consumer control validation owns handle state and structural inputs.

use std::collections::BTreeSet;

use crate::consumer_action_validation::ConsumerStates;
use crate::{
    AssignedConsumerControl, AssignedConsumerControlAction, AssignedPartitionPosition, ConsumerId,
    OperationId, TopicPartitionIdentity,
};

pub(crate) fn validate(
    action: &AssignedConsumerControlAction,
    consumers: &mut ConsumerStates,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    let Some(state) =
        crate::consumer_action_validation::open(&action.consumer_id, consumers, problems)
    else {
        return;
    };
    if state.group.is_some() {
        problems.push(format!(
            "consumer {} is not a directly assigned consumer",
            action.consumer_id
        ));
    }
    if !operation_ids.insert(action.operation_id.clone()) {
        problems.push(format!("duplicate operation id {}", action.operation_id));
    }
    if !(1..=60_000).contains(&action.timeout_ms) {
        problems.push(format!(
            "assigned control {} timeout_ms must be between 1 and 60000",
            action.operation_id
        ));
    }
    validate_control(
        &action.consumer_id,
        &action.control,
        state.assigned,
        problems,
    );
    if matches!(
        action.control,
        AssignedConsumerControl::Replace { .. } | AssignedConsumerControl::Add { .. }
    ) {
        state.assigned = true;
    }
}

fn validate_control(
    consumer_id: &ConsumerId,
    control: &AssignedConsumerControl,
    assigned: bool,
    problems: &mut Vec<String>,
) {
    match control {
        AssignedConsumerControl::Replace { partitions } => {
            validate_positioned(consumer_id, partitions, problems);
        }
        AssignedConsumerControl::Add { partitions } => {
            require_assigned(consumer_id, assigned, problems);
            validate_positioned(consumer_id, partitions, problems);
        }
        AssignedConsumerControl::Remove { partitions } => {
            require_assigned(consumer_id, assigned, problems);
            validate_identities(consumer_id, partitions, problems);
        }
        AssignedConsumerControl::Seek {
            partition,
            position,
        } => {
            require_assigned(consumer_id, assigned, problems);
            validate_identity(consumer_id, partition, problems);
            validate_offset(consumer_id, *position, problems);
        }
        AssignedConsumerControl::Pause { partition }
        | AssignedConsumerControl::Resume { partition } => {
            require_assigned(consumer_id, assigned, problems);
            validate_identity(consumer_id, partition, problems);
        }
    }
}

fn validate_positioned(
    consumer_id: &ConsumerId,
    partitions: &[AssignedPartitionPosition],
    problems: &mut Vec<String>,
) {
    let identities = partitions
        .iter()
        .map(|entry| TopicPartitionIdentity {
            topic: entry.topic.clone(),
            partition: entry.partition,
        })
        .collect::<Vec<_>>();
    validate_identities(consumer_id, &identities, problems);
    for entry in partitions {
        validate_offset(consumer_id, entry.position, problems);
    }
}

fn validate_identities(
    consumer_id: &ConsumerId,
    partitions: &[TopicPartitionIdentity],
    problems: &mut Vec<String>,
) {
    let mut unique = BTreeSet::new();
    for partition in partitions {
        validate_identity(consumer_id, partition, problems);
        if !unique.insert((partition.topic.as_str(), partition.partition)) {
            problems.push(format!(
                "consumer {consumer_id} control repeats partition {}:{}",
                partition.topic, partition.partition
            ));
        }
    }
}

fn validate_identity(
    consumer_id: &ConsumerId,
    partition: &TopicPartitionIdentity,
    problems: &mut Vec<String>,
) {
    if partition.topic.is_empty() || partition.topic.len() > 249 {
        problems.push(format!("consumer {consumer_id} control has invalid topic"));
    }
    if partition.partition < 0 {
        problems.push(format!(
            "consumer {consumer_id} control has negative partition {}",
            partition.partition
        ));
    }
}

fn validate_offset(
    consumer_id: &ConsumerId,
    position: crate::AssignedStartPosition,
    problems: &mut Vec<String>,
) {
    if let crate::AssignedStartPosition::Offset { offset } = position
        && offset < 0
    {
        problems.push(format!(
            "consumer {consumer_id} control has negative offset {offset}"
        ));
    }
}

fn require_assigned(consumer_id: &ConsumerId, assigned: bool, problems: &mut Vec<String>) {
    if !assigned {
        problems.push(format!(
            "consumer {consumer_id} control requires an active assignment"
        ));
    }
}
