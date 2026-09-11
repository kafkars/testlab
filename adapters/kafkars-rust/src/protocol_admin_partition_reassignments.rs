//! Partition-reassignment Admin calls preserve caller order and exact broker lists.

use std::collections::BTreeSet;
use std::io::Write;
use std::time::Duration;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, AdminPartitionReassignmentOutcome,
    AdminPartitionReassignmentsAlteration, AdminPartitionReassignmentsListing,
    AlterPartitionReassignmentsCommand, CommandId, ListPartitionReassignmentsCommand,
    PartitionReassignmentSnapshot,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::kafkars_api::{PartitionReassignmentChange, TopicPartition};
use crate::protocol::emit;
use crate::protocol_admin_read::{deadline_after, retry_safe};
use crate::state::AdapterState;

pub(crate) fn dispatch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    match command {
        AdapterCommand::AlterPartitionReassignments(command) => {
            alter(state, writer, command_id, command)
        }
        AdapterCommand::ListPartitionReassignments(command) => {
            list(state, writer, command_id, command)
        }
        _ => Err(invalid(
            "non-partition-reassignment command reached dispatcher",
        )),
    }
}

fn alter<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AlterPartitionReassignmentsCommand,
) -> Result<(), AdapterError> {
    let changes = command
        .changes
        .iter()
        .map(|change| {
            PartitionReassignmentChange::new(
                change.topic.clone(),
                change.partition,
                change.replicas.clone(),
            )
        })
        .collect::<Vec<_>>();
    let result = state
        .client(&command.client_id)?
        .admin()
        .alter_partition_reassignments(changes)
        .allow_replication_factor_change(command.allow_replication_factor_change)
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let throttle_time_ms = throttle(result.throttle_time())?;
    let entries = result.into_partitions().into_entries();
    if entries.len() != command.changes.len() {
        return Err(invalid("mutation returned the wrong outcome count"));
    }
    let outcomes = entries
        .into_iter()
        .zip(&command.changes)
        .map(|((target, result), expected)| {
            if target.topic() != expected.topic || target.partition() != expected.partition {
                return Err(invalid("mutation changed caller partition order"));
            }
            Ok(AdminPartitionReassignmentOutcome {
                topic: target.topic().to_owned(),
                partition: target.partition(),
                error_code: result
                    .err()
                    .map(|error| crate::normalize::error_code(&error)),
            })
        })
        .collect::<Result<Vec<_>, AdapterError>>()?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::PartitionReassignmentsAltered(AdminPartitionReassignmentsAlteration {
                operation_id: command.operation_id,
                throttle_time_ms,
                outcomes,
            }),
        ),
    )
}

fn list<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: ListPartitionReassignmentsCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let targets = command.targets.as_ref().map(|targets| {
        targets
            .iter()
            .map(|target| TopicPartition::new(target.topic.clone(), target.partition))
            .collect::<Vec<_>>()
    });
    let result = retry_until_with_remaining(
        deadline,
        |remaining| match &targets {
            Some(targets) => client
                .admin()
                .list_partition_reassignments(targets.clone())
                .deadline_after(remaining)
                .submit()
                .wait(),
            None => client
                .admin()
                .list_all_partition_reassignments()
                .deadline_after(remaining)
                .submit()
                .wait(),
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let throttle_time_ms = throttle(result.throttle_time())?;
    let rows = result.into_reassignments();
    validate_listing_order(targets.as_deref(), &rows)?;
    let reassignments = rows
        .into_iter()
        .map(|(target, reassignment)| {
            let snapshot = PartitionReassignmentSnapshot {
                topic: target.topic().to_owned(),
                partition: target.partition(),
                replicas: reassignment.replicas().to_vec(),
                adding_replicas: reassignment.adding_replicas().to_vec(),
                removing_replicas: reassignment.removing_replicas().to_vec(),
            };
            validate_snapshot(&snapshot)?;
            Ok(snapshot)
        })
        .collect::<Result<Vec<_>, AdapterError>>()?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::PartitionReassignmentsListed(AdminPartitionReassignmentsListing {
                operation_id: command.operation_id,
                throttle_time_ms,
                reassignments,
            }),
        ),
    )
}

fn validate_listing_order(
    selected: Option<&[TopicPartition]>,
    rows: &[(TopicPartition, crate::kafkars_api::PartitionReassignment)],
) -> Result<(), AdapterError> {
    let actual = rows.iter().map(|(target, _)| target).collect::<Vec<_>>();
    let ordered = if let Some(selected) = selected {
        selected
            .iter()
            .filter(|target| actual.contains(target))
            .eq(actual.iter().copied())
    } else {
        actual.windows(2).all(|pair| {
            pair[0].topic().as_bytes() < pair[1].topic().as_bytes()
                || (pair[0].topic() == pair[1].topic() && pair[0].partition() < pair[1].partition())
        })
    };
    if !ordered {
        return Err(invalid("listing returned noncanonical partition order"));
    }
    Ok(())
}

fn validate_snapshot(value: &PartitionReassignmentSnapshot) -> Result<(), AdapterError> {
    if value.topic.is_empty()
        || value.partition < 0
        || !valid_replicas(&value.replicas, false)
        || !valid_replicas(&value.adding_replicas, true)
        || !valid_replicas(&value.removing_replicas, true)
    {
        return Err(invalid("listing returned malformed reassignment state"));
    }
    Ok(())
}

fn valid_replicas(replicas: &[i32], allow_empty: bool) -> bool {
    (allow_empty || !replicas.is_empty())
        && replicas.iter().all(|replica| *replica >= 0)
        && replicas.iter().copied().collect::<BTreeSet<_>>().len() == replicas.len()
}

fn throttle(value: Duration) -> Result<u64, AdapterError> {
    u64::try_from(value.as_millis()).map_err(|_| invalid("returned unrepresentable throttle time"))
}

fn invalid(detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("partition reassignment Admin {detail}"))
}
