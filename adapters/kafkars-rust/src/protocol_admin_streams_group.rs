//! One bounded lifecycle qualifies every public Streams-group Admin method.

use std::io::Write;
use std::time::{Duration, Instant};

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminStreamsGroupAdminLifecycle,
    AdminStreamsGroupPartition, CommandId, ExerciseStreamsGroupAdminLifecycleCommand,
};

use crate::AdapterError;
use crate::kafkars_api::{
    ConsumerGroupOffsetAlteration, ErrorKind, KafkaError, ListStreamsGroupOffsetsQuery,
    TopicPartition,
};
use crate::protocol::emit;
use crate::protocol_admin_streams_group_result as result;
use crate::state::AdapterState;

#[allow(
    clippy::too_many_lines,
    reason = "the bounded Streams-group lifecycle keeps its ordered public calls explicit"
)]
pub(crate) fn exercise<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: ExerciseStreamsGroupAdminLifecycleCommand,
) -> Result<(), AdapterError> {
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(command.timeout_ms))
        .ok_or_else(|| invalid(&command, "deadline overflowed"))?;
    let client = state.client(&command.client_id)?;
    let admin = client.admin();
    let primary = command.primary_group_id.clone();
    let group_order = vec![command.secondary_group_id.clone(), primary.clone()];
    let partition = || TopicPartition::new(command.input_topic.clone(), 0);
    let mut throttles = [0; 9];

    let described = admin
        .describe_streams_group(primary.clone())
        .include_authorized_operations(command.include_authorized_operations)
        .include_topology_description(command.include_topology_description)
        .deadline_after(remaining(deadline)?)
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let (throttle, singular_description) =
        result::singular_description(&described, &command.operation_id)?;
    throttles[0] = throttle;

    let described = admin
        .describe_streams_groups(group_order.clone())
        .include_authorized_operations(command.include_authorized_operations)
        .include_topology_description(command.include_topology_description)
        .deadline_after(remaining(deadline)?)
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let (throttle, plural_descriptions) =
        result::plural_descriptions(described, &group_order, &command.operation_id)?;
    throttles[1] = throttle;

    let listed = admin
        .list_streams_group_offsets(primary.clone())
        .partitions([partition()])
        .require_stable(command.require_stable)
        .deadline_after(remaining(deadline)?)
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let (throttle, singular_initial_offset) = result::singular_offset(
        listed,
        &primary,
        &command.input_topic,
        0,
        &command.operation_id,
        "list-one",
    )?;
    throttles[2] = throttle;

    let queries = group_order
        .iter()
        .map(|group_id| ListStreamsGroupOffsetsQuery::selected(group_id.clone(), [partition()]));
    let listed = admin
        .list_streams_groups_offsets(queries)
        .require_stable(command.require_stable)
        .deadline_after(remaining(deadline)?)
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let (throttle, plural_initial_offsets) = result::plural_offsets(
        listed,
        &group_order,
        &command.input_topic,
        0,
        &command.operation_id,
    )?;
    throttles[3] = throttle;

    let altered = admin
        .alter_streams_group_offsets(
            primary.clone(),
            [ConsumerGroupOffsetAlteration::new(
                command.input_topic.clone(),
                0,
                command.altered_offset,
            )],
        )
        .deadline_after(remaining(deadline)?)
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    throttles[4] = result::throttle_ms(
        altered.throttle_time(),
        &command.operation_id,
        "alter-offset",
    )?;
    result::partition_success(
        altered.into_offsets().into_entries(),
        &command.input_topic,
        0,
        &command.operation_id,
        "Streams-group offset alteration",
    )?;

    let listed = list_selected(&admin, &command, &primary, deadline)?;
    let (throttle, offset_after_alter) = result::singular_offset(
        listed,
        &primary,
        &command.input_topic,
        0,
        &command.operation_id,
        "list-after-alter",
    )?;
    throttles[5] = throttle;

    let deleted = admin
        .delete_streams_group_offsets(primary.clone(), [partition()])
        .deadline_after(remaining(deadline)?)
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    throttles[6] = result::throttle_ms(
        deleted.throttle_time(),
        &command.operation_id,
        "delete-offset",
    )?;
    result::partition_success(
        deleted.into_offsets().into_entries(),
        &command.input_topic,
        0,
        &command.operation_id,
        "Streams-group offset deletion",
    )?;
    let deleted_offset = AdminStreamsGroupPartition {
        group_id: primary.clone(),
        topic: command.input_topic.clone(),
        partition: 0,
    };

    let listed = list_selected(&admin, &command, &primary, deadline)?;
    let (throttle, offset_after_delete) = result::singular_offset(
        listed,
        &primary,
        &command.input_topic,
        0,
        &command.operation_id,
        "list-after-delete",
    )?;
    throttles[7] = throttle;

    let deleted = admin
        .delete_streams_groups(group_order.clone())
        .deadline_after(remaining(deadline)?)
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    throttles[8] = result::throttle_ms(
        deleted.throttle_time(),
        &command.operation_id,
        "delete-groups",
    )?;
    result::groups_success(
        deleted.into_groups().into_entries(),
        &group_order,
        &command.operation_id,
    )?;

    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::StreamsGroupAdminLifecycleExercised(AdminStreamsGroupAdminLifecycle {
                operation_id: command.operation_id,
                singular_description,
                plural_descriptions,
                singular_initial_offset,
                plural_initial_offsets,
                offset_after_alter,
                deleted_offset,
                offset_after_delete,
                deleted_group_ids: group_order,
                throttle_times_ms: throttles,
            }),
        ),
    )
}

fn list_selected(
    admin: &crate::kafkars_api::Admin,
    command: &ExerciseStreamsGroupAdminLifecycleCommand,
    group_id: &str,
    deadline: Instant,
) -> Result<crate::kafkars_api::ListStreamsGroupOffsetsResult, AdapterError> {
    admin
        .list_streams_group_offsets(group_id.to_owned())
        .partitions([TopicPartition::new(command.input_topic.clone(), 0)])
        .require_stable(command.require_stable)
        .deadline_after(remaining(deadline)?)
        .submit()
        .wait()
        .map_err(AdapterError::Client)
}

fn remaining(deadline: Instant) -> Result<Duration, AdapterError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(AdapterError::Client(KafkaError::new(
            ErrorKind::Timeout,
            "Streams-group Admin lifecycle deadline elapsed",
        )))
    } else {
        Ok(remaining)
    }
}

fn invalid(command: &ExerciseStreamsGroupAdminLifecycleCommand, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {} {detail}", command.operation_id))
}
