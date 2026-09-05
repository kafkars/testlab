//! Group assignment observation drains public transitions and requires a stable fixed point.

use std::collections::BTreeSet;
use std::io::Write;
use std::time::{Duration, Instant};

use crate::kafkars_api::{ConsumerAssignment, ConsumerEvent, ErrorKind, RetryAdvice};
use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, CommandId, ConsumerId, GroupAssignmentTransition,
    GroupAssignmentTransitionKind, GroupAssignmentsObservation, GroupConsumerAssignment,
    ObserveGroupAssignmentsCommand, TopicPartitionIdentity,
};

use crate::AdapterError;
use crate::admission_retry::retry_until;
use crate::protocol::emit;
use crate::state::AdapterState;

const POLL_SLICE: Duration = Duration::from_millis(10);
const MAX_PENDING_TRANSITIONS: usize = 256;

pub(crate) fn record_transition(
    transitions: &mut Vec<GroupAssignmentTransition>,
    transition: GroupAssignmentTransition,
) -> Result<(), AdapterError> {
    if transitions.len() >= MAX_PENDING_TRANSITIONS {
        return Err(AdapterError::ConsumerRecord(
            "group transition evidence capacity exceeded".to_owned(),
        ));
    }
    transitions.push(transition);
    Ok(())
}

pub(crate) fn take_pending_transitions(
    pending: &mut Vec<GroupAssignmentTransition>,
    members: &BTreeSet<ConsumerId>,
) -> Vec<GroupAssignmentTransition> {
    let (selected, retained) = std::mem::take(pending)
        .into_iter()
        .partition(|transition| members.contains(&transition.consumer_id));
    *pending = retained;
    selected
}

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
    let groups = command
        .consumer_ids
        .iter()
        .map(|id| {
            state
                .group_consumer_mut(id)
                .map(|consumer| consumer.group_id().to_owned())
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    let members = command
        .consumer_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let require_transition =
        membership_changed(state.observed_group_members.get(&groups), &members);
    let mut transitions = take_pending_transitions(&mut state.pending_group_transitions, &members);
    let mut previous = None;
    let assignments = loop {
        let drained = drain_transitions(state, &command.consumer_ids, deadline, &mut transitions)?;
        let current = if drained {
            snapshots(state, &command.consumer_ids)?
        } else {
            None
        };
        let stable = (!require_transition || !transitions.is_empty())
            && current.as_deref().is_some_and(stable_assignment_candidate);
        if stable && current == previous {
            break current.unwrap_or_default();
        }
        previous = current;
        if Instant::now() >= deadline {
            break Vec::new();
        }
        std::thread::sleep(POLL_SLICE);
    };
    if !assignments.is_empty() {
        state.observed_group_members.insert(groups, members);
    }
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

pub(super) fn membership_changed(
    previous: Option<&BTreeSet<ConsumerId>>,
    current: &BTreeSet<ConsumerId>,
) -> bool {
    previous != Some(current)
}

pub(super) fn stable_assignment_candidate(assignments: &[GroupConsumerAssignment]) -> bool {
    let total = assignments
        .iter()
        .map(|assignment| assignment.partitions.len())
        .sum::<usize>();
    if assignments.is_empty()
        || assignments
            .iter()
            .any(|assignment| assignment.partitions.is_empty())
    {
        return false;
    }
    let unique = assignments
        .iter()
        .flat_map(|assignment| &assignment.partitions)
        .collect::<BTreeSet<_>>();
    total == unique.len()
}

pub(crate) fn drain_transitions(
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
                ConsumerEvent::PartitionsAssigned(assignment) => record_transition(
                    transitions,
                    transition(
                        consumer_id,
                        GroupAssignmentTransitionKind::Assigned,
                        &assignment,
                    ),
                )?,
                ConsumerEvent::PartitionsLost(assignment) => record_transition(
                    transitions,
                    transition(
                        consumer_id,
                        GroupAssignmentTransitionKind::Lost,
                        &assignment,
                    ),
                )?,
                ConsumerEvent::PartitionsRevoking(mut revocation) => {
                    record_transition(
                        transitions,
                        transition(
                            consumer_id,
                            GroupAssignmentTransitionKind::Revoking,
                            revocation.assignment(),
                        ),
                    )?;
                    loop {
                        match revocation.complete() {
                            Ok(()) => break,
                            Err(error) if error.retry_advice() == RetryAdvice::RetrySafe => {
                                if Instant::now() >= deadline {
                                    return Ok(false);
                                }
                                std::thread::sleep(POLL_SLICE);
                            }
                            Err(error) => {
                                let consumer = state.group_consumer_mut(consumer_id)?;
                                let current = retry_until(
                                    deadline,
                                    || consumer.assignment(),
                                    |error| error.retry_advice() == RetryAdvice::RetrySafe,
                                )
                                .map_err(AdapterError::Client)?;
                                // A newer public fence proves this old lease no longer owns
                                // release. This does not claim its acknowledgment succeeded.
                                if superseded_revocation(
                                    error.kind(),
                                    revocation.assignment_epoch(),
                                    current.as_ref().map(ConsumerAssignment::assignment_epoch),
                                ) {
                                    break;
                                }
                                if error.kind() == ErrorKind::State && Instant::now() < deadline {
                                    std::thread::sleep(POLL_SLICE);
                                    continue;
                                }
                                return Err(AdapterError::Client(error));
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(true)
}

pub(super) fn superseded_revocation(
    kind: ErrorKind,
    revoked_epoch: u64,
    current_epoch: Option<u64>,
) -> bool {
    kind == ErrorKind::State && current_epoch.is_some_and(|current| current > revoked_epoch)
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
