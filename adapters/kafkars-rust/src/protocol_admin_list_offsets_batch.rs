//! Batched offset reads use one public call and preserve caller-ordered outcomes.

use std::io::Write;

use crate::kafkars_api::ListOffsetsQuery;
use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminOffsetListingOutcome, AdminOffsetPosition,
    AdminOffsetsListing, CommandId, ListOffsetsBatchCommand, OffsetListingSelection,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::protocol::emit;
use crate::protocol_admin_plural_result::{
    PartitionResult, ResourceResult, ordered_partition_results,
};
use crate::protocol_admin_read::{deadline_after, public_read_isolation, retry_safe};
use crate::state::AdapterState;

pub(crate) fn list<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: ListOffsetsBatchCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .list_offsets(public_queries(&command.queries))
                .read_isolation(public_read_isolation(command.read_isolation))
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let entries = result
        .into_offsets()
        .into_entries()
        .into_iter()
        .map(|(key, result)| (key, result.map(|value| value.offset())))
        .collect();
    let requested = command
        .queries
        .iter()
        .map(|query| (query.topic.clone(), query.partition))
        .collect::<Vec<_>>();
    let results =
        ordered_partition_results(entries, &requested, &command.operation_id, "offset listing")?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::OffsetsListed(AdminOffsetsListing {
                operation_id: command.operation_id,
                outcomes: outcomes(results),
            }),
        ),
    )
}

fn public_queries(
    queries: &[OffsetListingSelection],
) -> impl Iterator<Item = ListOffsetsQuery> + '_ {
    queries.iter().map(|query| {
        ListOffsetsQuery::new(
            query.topic.clone(),
            query.partition,
            offset_spec(query.position),
        )
    })
}

const fn offset_spec(position: AdminOffsetPosition) -> crate::kafkars_api::OffsetSpec {
    match position {
        AdminOffsetPosition::Earliest => crate::kafkars_api::OffsetSpec::earliest(),
        AdminOffsetPosition::Latest => crate::kafkars_api::OffsetSpec::latest(),
    }
}

pub(crate) fn outcomes(
    results: Vec<PartitionResult<Option<i64>>>,
) -> Vec<AdminOffsetListingOutcome> {
    results
        .into_iter()
        .map(|result| match result.result {
            ResourceResult::Success(offset) => AdminOffsetListingOutcome {
                topic: result.topic,
                partition: result.partition,
                offset,
                error_code: None,
            },
            ResourceResult::Failure(error_code) => AdminOffsetListingOutcome {
                topic: result.topic,
                partition: result.partition,
                offset: None,
                error_code: Some(error_code),
            },
        })
        .collect()
}
