//! Group assignment observation drains public transitions and requires a stable fixed point.

use std::io::Write;
use std::time::{Duration, Instant};

use crate::kafkars_api::{ConsumerAssignment, ConsumerEvent, RetryAdvice};
use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, CommandId, ConsumerId, GroupAssignmentTransition,
    GroupAssignmentTransitionKind, GroupAssignmentsObservation, GroupConsumerAssignment,
    ObserveGroupAssignmentsCommand, TopicPartitionIdentity,
};

use crate::AdapterError;
use crate::protocol::emit;
use crate::state::AdapterState;

const POLL_SLICE: Duration = Duration::from_millis(10);

pub(crate) fn observe<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: ObserveGroupAssignmentsCommand,
) -> Result<(), AdapterError> {
    let started = Instant::now();
    let deadline = started
        .checked_add(Duration::from_millis(command.timeout_ms))
        .unwrap_or(started);
    let mut transitions = Vec::new();
    let mut previous = None;
    let assignments = loop {
        let drained = drain_transitions(state, &command.consumer_ids, deadline, &mut transitions)?;
        let current = if drained {
            snapshots(state, &command.consumer_ids)?
        } else {
            None
        };
        if current.is_some() && current == previous {
            break current.unwrap_or_default();
        }
        previous = current;
        if Instant::now() >= deadline {
            break Vec::new();
        }
        std::thread::sleep(POLL_SLICE);
    };
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::GroupAssignmentsObserved(GroupAssignmentsObservation {
                operation_id: command.operation_id,
                transitions,
                assignments,
            }),
        ),
    )
}

fn drain_transitions(
    state: &mut AdapterState,
    consumer_ids: &[ConsumerId],
    deadline: Instant,
    transitions: &mut Vec<GroupAssignmentTransition>,
) -> Result<bool, AdapterError> {
    for consumer_id in consumer_ids {
        loop {
            let event = match state.group_consumer_mut(consumer_id)?.try_take_event() {
                Ok(event) => event,
                Err(error) if error.retry_advice() == RetryAdvice::RetrySafe => return Ok(false),
                Err(error) => return Err(AdapterError::Client(error)),
            };
            let Some(event) = event else {
                break;
            };
            match event {
                ConsumerEvent::PartitionsAssigned(assignment) => transitions.push(transition(
                    consumer_id,
                    GroupAssignmentTransitionKind::Assigned,
                    &assignment,
                )),
                ConsumerEvent::PartitionsLost(assignment) => transitions.push(transition(
                    consumer_id,
                    GroupAssignmentTransitionKind::Lost,
                    &assignment,
                )),
                ConsumerEvent::PartitionsRevoking(mut revocation) => {
                    transitions.push(transition(
                        consumer_id,
                        GroupAssignmentTransitionKind::Revoking,
                        revocation.assignment(),
                    ));
                    loop {
                        match revocation.complete() {
                            Ok(()) => break,
                            Err(error) if error.retry_advice() == RetryAdvice::RetrySafe => {
                                if Instant::now() >= deadline {
                                    return Ok(false);
                                }
                                std::thread::sleep(POLL_SLICE);
                            }
                            Err(error) => return Err(AdapterError::Client(error)),
                        }
                    }
                }
            }
        }
    }
    Ok(true)
}

fn snapshots(
    state: &mut AdapterState,
    consumer_ids: &[ConsumerId],
) -> Result<Option<Vec<GroupConsumerAssignment>>, AdapterError> {
    let mut snapshots = Vec::with_capacity(consumer_ids.len());
    for consumer_id in consumer_ids {
        let consumer = state.group_consumer_mut(consumer_id)?;
        let assignment = match consumer.assignment() {
            Ok(Some(assignment)) => assignment,
            Ok(None) => return Ok(None),
            Err(error) if error.retry_advice() == RetryAdvice::RetrySafe => return Ok(None),
            Err(error) => return Err(AdapterError::Client(error)),
        };
        let metadata = match consumer.group_metadata() {
            Ok(Some(metadata)) => metadata,
            Ok(None) => return Ok(None),
            Err(error) if error.retry_advice() == RetryAdvice::RetrySafe => return Ok(None),
            Err(error) => return Err(AdapterError::Client(error)),
        };
        if assignment.assignment_epoch() != metadata.assignment_epoch() {
            return Ok(None);
        }
        snapshots.push(GroupConsumerAssignment {
            consumer_id: consumer_id.clone(),
            group_id: metadata.group_id().to_owned(),
            member_id: metadata.member_id().to_owned(),
            group_epoch: crate::protocol_group::normalize_group_epoch(metadata.membership_epoch()),
            assignment_epoch: assignment.assignment_epoch(),
            partitions: normalize_partitions(&assignment),
        });
    }
    Ok(Some(snapshots))
}

fn transition(
    consumer_id: &ConsumerId,
    kind: GroupAssignmentTransitionKind,
    assignment: &ConsumerAssignment,
) -> GroupAssignmentTransition {
    GroupAssignmentTransition {
        consumer_id: consumer_id.clone(),
        kind,
        assignment_epoch: assignment.assignment_epoch(),
        partitions: normalize_partitions(assignment),
    }
}

fn normalize_partitions(assignment: &ConsumerAssignment) -> Vec<TopicPartitionIdentity> {
    assignment
        .partitions()
        .iter()
        .map(|partition| TopicPartitionIdentity {
            topic: partition.topic().to_owned(),
            partition: partition.partition(),
        })
        .collect()
}
