//! Cluster-admin reads expose only packaged public identity and broker facts.

use std::io::Write;
use std::time::{Duration, Instant};

use crate::kafkars_api::{ClusterBroker, RetryAdvice};
use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminClusterDescription, CommandId, DescribeClusterCommand,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::protocol::emit;
use crate::protocol_admin_result::sorted_unique_nonnegative;
use crate::state::AdapterState;

pub(crate) fn describe<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DescribeClusterCommand,
) -> Result<(), AdapterError> {
    let started = Instant::now();
    let deadline = started
        .checked_add(Duration::from_millis(command.timeout_ms))
        .unwrap_or(started);
    let client = state.client(&command.client_id)?;
    let description = retry_until_with_remaining(
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
        |error| error.retry_advice() == RetryAdvice::RetrySafe,
    )
    .map_err(AdapterError::Client)?;
    let broker_ids = description
        .brokers()
        .iter()
        .map(ClusterBroker::id)
        .collect();
    let broker_ids =
        sorted_unique_nonnegative(broker_ids, &command.operation_id, "cluster broker")?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ClusterDescribed(AdminClusterDescription {
                operation_id: command.operation_id,
                cluster_id: Some(description.cluster_id().to_owned()),
                broker_ids,
            }),
        ),
    )
}
