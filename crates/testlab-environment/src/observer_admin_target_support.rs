//! Shared Admin-target identity and observation helpers.

use std::collections::BTreeSet;

use testlab_schema::OperationId;

use crate::observer_error::ObserverError;

pub(super) fn unique<T: Ord>(
    values: &[T],
    operation_id: &OperationId,
    resource: &str,
) -> Result<(), ObserverError> {
    let mut seen = BTreeSet::new();
    if values.iter().any(|value| !seen.insert(value)) {
        return Err(invalid(
            operation_id,
            format!("contains duplicate {resource}"),
        ));
    }
    Ok(())
}

pub(super) fn invalid(operation_id: &OperationId, detail: impl std::fmt::Display) -> ObserverError {
    ObserverError::InvalidTarget(format!("admin operation {operation_id} {detail}"))
}

pub(super) fn ordinal(first: u64, index: usize) -> Result<u64, ObserverError> {
    first
        .checked_add(u64::try_from(index).map_err(|_| ObserverError::ObservationOverflow)?)
        .ok_or(ObserverError::ObservationOverflow)
}
