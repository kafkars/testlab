//! Independent watermark queries prove selected offsets and record deletion.

use std::thread;
use std::time::Duration;

use testlab_schema::{BrokerPartitionOffsets, BrokerStateObservation};

use crate::observer::remaining;
use crate::observer_admin::{AdminObserverRequest, client};
use crate::observer_admin_target::PartitionOffsetsTarget;
use crate::observer_error::ObserverError;

const POLL_SLICE: Duration = Duration::from_millis(50);

pub(super) fn capture(
    request: AdminObserverRequest<'_>,
    target: &PartitionOffsetsTarget,
) -> Result<BrokerStateObservation, ObserverError> {
    let admin = client(request, "partition-offsets")?;
    loop {
        let (low_watermark, high_watermark) = admin.inner().fetch_watermarks(
            &target.topic,
            target.partition,
            remaining(request.deadline)?,
        )?;
        if low_watermark < 0 || high_watermark < low_watermark {
            return Err(ObserverError::InvalidBrokerState(format!(
                "watermarks for {}[{}] were {low_watermark}..{high_watermark}",
                target.topic, target.partition
            )));
        }
        let observed = BrokerStateObservation::PartitionOffsets(BrokerPartitionOffsets {
            observation: request.first_observation,
            operation_id: target.operation_id.clone(),
            topic: target.topic.clone(),
            partition: target.partition,
            low_watermark,
            high_watermark,
        });
        if !target.poll_expected || matches_expected(low_watermark, high_watermark, target) {
            return Ok(observed);
        }
        let wait = request
            .deadline
            .saturating_duration_since(std::time::Instant::now());
        if wait.is_zero() {
            return Err(ObserverError::Deadline);
        }
        thread::sleep(POLL_SLICE.min(wait));
    }
}

fn matches_expected(low: i64, high: i64, target: &PartitionOffsetsTarget) -> bool {
    target.expected_low.is_none_or(|expected| low == expected)
        && target.expected_high.is_none_or(|expected| high == expected)
}
