//! Absolute deadlines prevent setup and cleanup from escaping scenario bounds.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::run_error::RunFailure;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Deadline {
    end: Instant,
}

impl Deadline {
    pub(crate) fn after_millis(timeout_ms: u64) -> Result<Self, RunFailure> {
        let duration = Duration::from_millis(timeout_ms);
        let Some(end) = Instant::now().checked_add(duration) else {
            return Err(RunFailure::harness(
                "deadline_overflow",
                "scenario deadline overflowed the monotonic clock",
            ));
        };
        Ok(Self { end })
    }

    pub(crate) fn remaining(self) -> Result<Duration, RunFailure> {
        self.end
            .checked_duration_since(Instant::now())
            .ok_or_else(|| RunFailure::harness("scenario_timeout", "scenario deadline elapsed"))
    }
}

pub(crate) fn unix_ms() -> Result<u64, RunFailure> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            RunFailure::harness(
                "system_clock_before_epoch",
                format!("system clock is before Unix epoch: {error}"),
            )
        })?;
    u64::try_from(elapsed.as_millis()).map_err(|_| {
        RunFailure::harness(
            "system_clock_overflow",
            "Unix millisecond timestamp exceeded u64",
        )
    })
}
