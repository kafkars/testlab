//! Cluster feature discovery exposes exact generated-free public metadata.

use std::io::Write;
use std::time::{Duration, Instant};

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminFeaturesDescription, CommandId,
    DescribeFeaturesCommand, FeatureVersionRange, OperationId,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::kafkars_api::RetryAdvice;
use crate::protocol::emit;
use crate::state::AdapterState;

pub(crate) fn describe<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DescribeFeaturesCommand,
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
                .describe_features()
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        |error| error.retry_advice() == RetryAdvice::RetrySafe,
    )
    .map_err(AdapterError::Client)?;
    let (_, supported, complete, epoch, finalized, zk_migration_ready) = description.into_parts();
    let supported_features = supported
        .into_iter()
        .map(|feature| {
            let (name, min_version_level, max_version_level) = feature.into_parts();
            FeatureVersionRange {
                name,
                min_version_level,
                max_version_level,
            }
        })
        .collect::<Vec<_>>();
    let finalized_features = finalized
        .into_iter()
        .map(|feature| {
            let (name, min_version_level, max_version_level) = feature.into_parts();
            FeatureVersionRange {
                name,
                min_version_level,
                max_version_level,
            }
        })
        .collect::<Vec<_>>();
    validate_ranges(
        &supported_features,
        false,
        &command.operation_id,
        "supported",
    )?;
    validate_ranges(
        &finalized_features,
        true,
        &command.operation_id,
        "finalized",
    )?;
    if epoch.is_some_and(|value| value < 0) {
        return Err(invalid(
            &command.operation_id,
            "returned a negative finalized-feature epoch",
        ));
    }
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::FeaturesDescribed(AdminFeaturesDescription {
                operation_id: command.operation_id,
                supported_features,
                supported_features_complete: complete,
                finalized_features_epoch: epoch,
                finalized_features,
                zk_migration_ready,
            }),
        ),
    )
}

fn validate_ranges(
    ranges: &[FeatureVersionRange],
    empty_allowed: bool,
    operation_id: &OperationId,
    kind: &str,
) -> Result<(), AdapterError> {
    if !empty_allowed && ranges.is_empty() {
        return Err(invalid(
            operation_id,
            "returned no supported feature ranges",
        ));
    }
    if ranges.iter().any(|range| {
        range.name.is_empty()
            || range.min_version_level < 0
            || range.max_version_level < range.min_version_level
    }) || ranges
        .windows(2)
        .any(|pair| pair[0].name.as_bytes() >= pair[1].name.as_bytes())
    {
        return Err(invalid(
            operation_id,
            &format!("returned noncanonical {kind} feature ranges"),
        ));
    }
    Ok(())
}

fn invalid(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
