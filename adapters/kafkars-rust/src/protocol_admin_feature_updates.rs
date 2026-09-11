//! Validation-only finalized-feature updates retain ordered public outcomes.

use std::io::Write;
use std::time::Duration;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminFeatureUpdateOutcome, AdminFeatureUpdatesValidation,
    CommandId, FeatureUpdateKind, ValidateFeatureUpdatesCommand,
};

use crate::AdapterError;
use crate::kafkars_api::FeatureUpdate;
use crate::protocol::emit;
use crate::state::AdapterState;

pub(crate) fn validate<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: ValidateFeatureUpdatesCommand,
) -> Result<(), AdapterError> {
    let updates = command
        .updates
        .iter()
        .map(|update| match update.kind {
            FeatureUpdateKind::Upgrade => {
                FeatureUpdate::upgrade(update.name.clone(), update.max_version_level)
            }
            FeatureUpdateKind::SafeDowngrade => {
                FeatureUpdate::safe_downgrade(update.name.clone(), update.max_version_level)
            }
            FeatureUpdateKind::UnsafeDowngrade => {
                FeatureUpdate::unsafe_downgrade(update.name.clone(), update.max_version_level)
            }
        })
        .collect();
    let result = state
        .client(&command.client_id)?
        .admin()
        .update_features(updates)
        .validate_only(true)
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let throttle_time_ms = u64::try_from(result.throttle_time().as_millis())
        .map_err(|_| invalid(&command, "returned an unrepresentable throttle time"))?;
    let entries = result.into_features().into_entries();
    if entries.len() != command.updates.len() {
        return Err(invalid(&command, "returned the wrong outcome count"));
    }
    let outcomes = entries
        .into_iter()
        .zip(&command.updates)
        .map(|((name, outcome), requested)| {
            if name != requested.name {
                return Err(invalid(&command, "changed caller feature order"));
            }
            Ok(AdminFeatureUpdateOutcome {
                name,
                error_code: outcome
                    .err()
                    .map(|error| crate::normalize::error_code(&error)),
            })
        })
        .collect::<Result<Vec<_>, AdapterError>>()?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::FeatureUpdatesValidated(AdminFeatureUpdatesValidation {
                operation_id: command.operation_id,
                throttle_time_ms,
                outcomes,
            }),
        ),
    )
}

fn invalid(command: &ValidateFeatureUpdatesCommand, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {} {detail}", command.operation_id))
}
