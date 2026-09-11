//! Replica log-directory discovery preserves every selected broker and placement fact.

use std::io::Write;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminReplicaLogDirDescription,
    AdminReplicaLogDirsDescription, CommandId, DescribeReplicaLogDirsCommand,
    ReplicaLogDirIdentity, ReplicaLogDirLocationState,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::kafkars_api::{ClusterBroker, TopicPartitionReplica};
use crate::protocol::emit;
use crate::protocol_admin_read::{deadline_after, retry_safe};
use crate::protocol_admin_result::sorted_unique_nonnegative;
use crate::state::AdapterState;

pub(crate) fn describe<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DescribeReplicaLogDirsCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let cluster = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .describe_cluster()
                .include_fenced_brokers(false)
                .include_authorized_operations(false)
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let broker_ids = cluster.brokers().iter().map(ClusterBroker::id).collect();
    let mut broker_ids = sorted_unique_nonnegative(
        broker_ids,
        &command.operation_id,
        "replica log-directory broker",
    )?;
    if broker_ids.is_empty() {
        return Err(invalid(&command, "discovered no brokers"));
    }
    broker_ids.reverse();
    let requested = broker_ids
        .into_iter()
        .map(|broker_id| {
            TopicPartitionReplica::new(command.topic.clone(), command.partition, broker_id)
        })
        .collect::<Vec<_>>();
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .describe_replica_log_dirs(requested.clone())
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let throttle_time_ms = u64::try_from(result.throttle_time().as_millis())
        .map_err(|_| invalid(&command, "returned an unrepresentable throttle time"))?;
    let entries = result.into_replicas().into_entries();
    if entries.len() != requested.len() {
        return Err(invalid(
            &command,
            "returned a different number of replica outcomes than requested",
        ));
    }
    let mut replicas = Vec::with_capacity(entries.len());
    for ((replica, outcome), expected) in entries.into_iter().zip(&requested) {
        if &replica != expected {
            return Err(invalid(&command, "did not preserve caller replica order"));
        }
        let info = outcome.map_err(AdapterError::Client)?;
        let current = info.current().map(|location| ReplicaLogDirLocationState {
            path: location.path().to_owned(),
            offset_lag: location.offset_lag(),
        });
        let future = info.future().map(|location| ReplicaLogDirLocationState {
            path: location.path().to_owned(),
            offset_lag: location.offset_lag(),
        });
        if current
            .iter()
            .chain(future.iter())
            .any(|location| location.path.is_empty())
        {
            return Err(invalid(&command, "returned an empty placement path"));
        }
        replicas.push(AdminReplicaLogDirDescription {
            replica: ReplicaLogDirIdentity {
                topic: replica.topic().to_owned(),
                partition: replica.partition(),
                broker_id: replica.broker_id(),
            },
            current,
            future,
        });
    }
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ReplicaLogDirsDescribed(AdminReplicaLogDirsDescription {
                operation_id: command.operation_id,
                topic: command.topic,
                partition: command.partition,
                throttle_time_ms,
                replicas,
            }),
        ),
    )
}

fn invalid(command: &DescribeReplicaLogDirsCommand, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {} {detail}", command.operation_id))
}
