//! Ownership action validation binds member sets, partition sets, and operation identities.

use std::collections::BTreeSet;

use crate::consumer_action_validation::{ConsumerGroupState, open};
use crate::scenario_action_validation::ActionStates;
use crate::{ConsumerId, OperationId, ScenarioAction, TopicPartitionIdentity};

const MAX_PARTITIONS: usize = 128;
const MAX_MEMBERS: usize = 32;
const MAX_RECORDS: usize = 256;

pub(crate) fn validate(
    action: &ScenarioAction,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    match action {
        ScenarioAction::AssignBeginningBatch(action) => {
            if let Some(consumer) = open(&action.consumer_id, &mut state.consumers, problems) {
                consumer.assigned = true;
            }
            partitions(
                &action.partitions,
                &format!("consumer {} assignment", action.consumer_id),
                problems,
            );
            timeout(
                action.timeout_ms,
                "assignment",
                &action.consumer_id.to_string(),
                problems,
            );
        }
        ScenarioAction::ObserveGroupAssignments(action) => {
            identity(&action.operation_id, &mut state.operation_ids, problems);
            let group = member_set(&action.consumer_ids, state, problems);
            partitions(
                &action.partitions,
                &format!("assignment observation {}", action.operation_id),
                problems,
            );
            if let Some(group) = group {
                for partition in &action.partitions {
                    if partition.topic != group.topic {
                        problems.push(format!(
                            "assignment observation {} expected topic {}, group subscribes to {}",
                            action.operation_id, partition.topic, group.topic
                        ));
                    }
                }
            }
            timeout(
                action.timeout_ms,
                "assignment observation",
                &action.operation_id.to_string(),
                problems,
            );
        }
        ScenarioAction::GroupReceiveSet(action) => {
            identity(&action.receive_id, &mut state.operation_ids, problems);
            member_set(&action.consumer_ids, state, problems);
            expected_operations(
                &action.receive_id,
                &action.expected_operation_ids,
                &state.sends,
                problems,
            );
            timeout(
                action.timeout_ms,
                "group receive set",
                &action.receive_id.to_string(),
                problems,
            );
        }
        _ => {}
    }
}

fn member_set(
    consumer_ids: &[ConsumerId],
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) -> Option<ConsumerGroupState> {
    if consumer_ids.is_empty() || consumer_ids.len() > MAX_MEMBERS {
        problems.push(format!(
            "group member set must contain between 1 and {MAX_MEMBERS} consumers"
        ));
    }
    let mut unique = BTreeSet::new();
    let mut expected = None;
    for consumer_id in consumer_ids {
        if !unique.insert(consumer_id) {
            problems.push(format!("duplicate group consumer {consumer_id}"));
        }
        let Some(consumer) = open(consumer_id, &mut state.consumers, problems) else {
            continue;
        };
        let Some(group) = consumer.group.clone() else {
            problems.push(format!("consumer {consumer_id} is not a group consumer"));
            continue;
        };
        match &expected {
            Some(first) if first != &group => problems.push(format!(
                "consumer {consumer_id} does not share the member set group, topic, and protocol"
            )),
            None => expected = Some(group),
            _ => {}
        }
    }
    expected
}

fn partitions(values: &[TopicPartitionIdentity], owner: &str, problems: &mut Vec<String>) {
    if values.is_empty() || values.len() > MAX_PARTITIONS {
        problems.push(format!(
            "{owner} must contain between 1 and {MAX_PARTITIONS} partitions"
        ));
    }
    let mut unique = BTreeSet::new();
    for partition in values {
        if partition.topic.is_empty() || partition.topic.len() > 249 {
            problems.push(format!("{owner} has invalid topic"));
        }
        if partition.partition < 0 {
            problems.push(format!(
                "{owner} has negative partition {}",
                partition.partition
            ));
        }
        if !unique.insert(partition) {
            problems.push(format!(
                "{owner} repeats {}:{}",
                partition.topic, partition.partition
            ));
        }
    }
}

fn expected_operations(
    receive_id: &OperationId,
    expected: &[OperationId],
    sends: &BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    if expected.is_empty() || expected.len() > MAX_RECORDS {
        problems.push(format!(
            "group receive set {receive_id} must expect between 1 and {MAX_RECORDS} records"
        ));
    }
    let mut unique = BTreeSet::new();
    for operation_id in expected {
        if !unique.insert(operation_id) {
            problems.push(format!(
                "group receive set {receive_id} repeats expected operation {operation_id}"
            ));
        }
        if !sends.contains(operation_id) {
            problems.push(format!(
                "group receive set {receive_id} expects missing prior send {operation_id}"
            ));
        }
    }
}

fn identity(
    operation_id: &OperationId,
    identities: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    if !identities.insert(operation_id.clone()) {
        problems.push(format!("duplicate operation id {operation_id}"));
    }
}

fn timeout(timeout_ms: u64, kind: &str, identity: &str, problems: &mut Vec<String>) {
    if !(100..=60_000).contains(&timeout_ms) {
        problems.push(format!(
            "{kind} {identity} timeout_ms must be between 100 and 60000"
        ));
    }
}
