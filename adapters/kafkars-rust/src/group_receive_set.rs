//! Multi-member group receive round-robins public batches and commits every checkpoint.

use std::collections::BTreeMap;
use std::io::Write;
use std::time::{Duration, Instant};

use crate::kafkars_api::RetryAdvice;
use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, CommandId, ConsumedRecord, GroupReceiveMemberCompletion,
    GroupReceiveSetCommand, GroupReceiveSetCompletion,
};

use crate::AdapterError;
use crate::protocol::emit;
use crate::state::AdapterState;

const POLL_SLICE: Duration = Duration::from_millis(10);

pub(crate) fn receive<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: GroupReceiveSetCommand,
) -> Result<(), AdapterError> {
    let started = Instant::now();
    let deadline = started
        .checked_add(Duration::from_millis(command.timeout_ms))
        .unwrap_or(started);
    let mut records = command
        .consumer_ids
        .iter()
        .cloned()
        .map(|consumer_id| (consumer_id, Vec::new()))
        .collect::<BTreeMap<_, Vec<ConsumedRecord>>>();
    let mut observed = 0;
    while observed < command.record_count && Instant::now() < deadline {
        let mut progress = false;
        for consumer_id in &command.consumer_ids {
            if !crate::group_receive_events::drive(state, consumer_id, deadline)? {
                continue;
            }
            let batch = match state.group_consumer_mut(consumer_id)?.try_take_batch() {
                Ok(batch) => batch,
                Err(error) if error.retry_advice() == RetryAdvice::RetrySafe => None,
                Err(error) => return Err(AdapterError::Client(error)),
            };
            if let Some(batch) = batch {
                let batch_records =
                    crate::protocol_group::commit_batch(state, consumer_id, batch, deadline)?;
                observed += batch_records.len();
                let member_records = records.get_mut(consumer_id).ok_or_else(|| {
                    AdapterError::ConsumerRecord(format!(
                        "group receive set omitted consumer {consumer_id}"
                    ))
                })?;
                member_records.extend(batch_records);
                progress = true;
            }
        }
        if !progress {
            std::thread::sleep(POLL_SLICE);
        }
    }
    let mut members = Vec::with_capacity(command.consumer_ids.len());
    for consumer_id in command.consumer_ids {
        let group_epoch = crate::protocol_group::public_group_epoch(state, &consumer_id, deadline)?;
        members.push(GroupReceiveMemberCompletion {
            records: records.remove(&consumer_id).unwrap_or_default(),
            consumer_id,
            committed: true,
            group_epoch,
        });
    }
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::GroupReceiveSetCompleted(GroupReceiveSetCompletion {
                receive_id: command.receive_id,
                members,
            }),
        ),
    )
}
