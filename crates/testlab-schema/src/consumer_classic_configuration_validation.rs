//! Classic group timing remains explicit and representable at scenario boundaries.

use crate::{ConsumerId, GroupConsumerConfiguration, GroupProtocol};

pub(super) fn validate(
    consumer_id: &ConsumerId,
    protocol: GroupProtocol,
    configuration: &Option<GroupConsumerConfiguration>,
    problems: &mut Vec<String>,
) {
    let Some(configuration) = configuration.as_ref() else {
        return;
    };
    let timings = [
        (
            "classic_session_timeout_ms",
            configuration.classic_session_timeout_ms,
        ),
        (
            "classic_rebalance_timeout_ms",
            configuration.classic_rebalance_timeout_ms,
        ),
        (
            "classic_heartbeat_interval_ms",
            configuration.classic_heartbeat_interval_ms,
        ),
        (
            "classic_heartbeat_attempt_timeout_ms",
            configuration.classic_heartbeat_attempt_timeout_ms,
        ),
        (
            "classic_rejoin_backoff_ms",
            configuration.classic_rejoin_backoff_ms,
        ),
        (
            "classic_rejoin_attempt_timeout_ms",
            configuration.classic_rejoin_attempt_timeout_ms,
        ),
    ];
    if protocol != GroupProtocol::Classic {
        if configuration.classic_assignor.is_some() {
            problems.push(format!(
                "consumer {consumer_id} sets classic_assignor for a non-classic group"
            ));
        }
        for &(field, value) in &timings {
            if value.is_some() {
                problems.push(format!(
                    "consumer {consumer_id} sets {field} for a non-classic group"
                ));
            }
        }
    }
    for (field, value) in timings {
        if value.is_some_and(|milliseconds| !(1..=i32::MAX as u64).contains(&milliseconds)) {
            problems.push(format!(
                "consumer {consumer_id} {field} must be between 1 and {}",
                i32::MAX
            ));
        }
    }
}
