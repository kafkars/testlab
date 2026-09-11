//! Log-directory discovery preserves broker routing and every stable replica fact.

use std::io::Write;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminBrokerLogDirsDescription, AdminLogDirDescription,
    AdminLogDirsDescription, CommandId, DescribeLogDirsCommand, LogDirReplicaState, OperationId,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::kafkars_api::{ClusterBroker, TopicPartition};
use crate::protocol::emit;
use crate::protocol_admin_read::{deadline_after, retry_safe};
use crate::protocol_admin_result::sorted_unique_nonnegative;
use crate::state::AdapterState;

pub(crate) fn describe<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DescribeLogDirsCommand,
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
    let mut broker_ids =
        sorted_unique_nonnegative(broker_ids, &command.operation_id, "log-directory broker")?;
    broker_ids.reverse();
    let target = TopicPartition::new(command.topic.clone(), command.partition);
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .describe_log_dirs(broker_ids.clone())
                .partitions([target.clone()])
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let throttle_time_ms = u64::try_from(result.throttle_time().as_millis()).map_err(|_| {
        invalid(
            &command.operation_id,
            "returned an unrepresentable throttle time",
        )
    })?;
    let entries = result.into_brokers().into_entries();
    if !entries
        .iter()
        .map(|(broker_id, _)| *broker_id)
        .eq(broker_ids.iter().copied())
    {
        return Err(invalid(
            &command.operation_id,
            "did not preserve caller broker order",
        ));
    }
    let mut brokers = Vec::with_capacity(entries.len());
    for (broker_id, outcome) in entries {
        let directories = outcome.map_err(AdapterError::Client)?.into_entries();
        let mut log_dirs = Vec::with_capacity(directories.len());
        for (path, outcome) in directories {
            let description = outcome.map_err(AdapterError::Client)?;
            let replicas = description
                .replicas()
                .iter()
                .map(|replica| LogDirReplicaState {
                    topic: replica.topic_name().to_owned(),
                    partition: replica.partition_index(),
                    size_bytes: replica.partition_size(),
                    offset_lag: replica.offset_lag(),
                    is_future: replica.is_future(),
                })
                .collect();
            log_dirs.push(AdminLogDirDescription {
                path,
                total_bytes: description.total_bytes(),
                usable_bytes: description.usable_bytes(),
                is_cordoned: description.is_cordoned(),
                replicas,
            });
        }
        brokers.push(AdminBrokerLogDirsDescription {
            broker_id,
            log_dirs,
        });
    }
    validate(&command, &brokers)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::LogDirsDescribed(AdminLogDirsDescription {
                operation_id: command.operation_id,
                topic: command.topic,
                partition: command.partition,
                throttle_time_ms,
                brokers,
            }),
        ),
    )
}

fn validate(
    command: &DescribeLogDirsCommand,
    brokers: &[AdminBrokerLogDirsDescription],
) -> Result<(), AdapterError> {
    if brokers.is_empty() {
        return Err(invalid(&command.operation_id, "returned no brokers"));
    }
    for broker in brokers {
        if broker.log_dirs.is_empty()
            || broker.log_dirs.iter().any(|directory| {
                directory.path.is_empty()
                    || directory.total_bytes.is_some_and(|value| value < 0)
                    || directory.usable_bytes.is_some_and(|value| value < 0)
                    || directory.replicas.iter().any(|replica| {
                        replica.topic != command.topic
                            || replica.partition != command.partition
                            || replica.size_bytes < 0
                    })
            })
            || broker
                .log_dirs
                .windows(2)
                .any(|pair| pair[0].path.as_bytes() >= pair[1].path.as_bytes())
        {
            return Err(invalid(
                &command.operation_id,
                "returned malformed or noncanonical log-directory state",
            ));
        }
    }
    Ok(())
}

fn invalid(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
