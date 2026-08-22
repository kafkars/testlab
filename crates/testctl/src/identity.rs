//! Process-local identity allocation gives every attempt a unique portable name.

use std::sync::atomic::{AtomicU64, Ordering};

use testlab_schema::RunId;

use crate::run_error::AppError;

static RUN_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(crate) fn new_run_id(prefix: &str, started_unix_ms: u64) -> Result<RunId, AppError> {
    let sequence = RUN_SEQUENCE
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .map_err(|_| AppError::Catalog("run identity counter overflowed".to_owned()))?;
    RunId::new(format!(
        "{prefix}-{started_unix_ms}-{}-{sequence}",
        std::process::id()
    ))
    .map_err(|error| AppError::Catalog(format!("generated invalid run id: {error}")))
}
