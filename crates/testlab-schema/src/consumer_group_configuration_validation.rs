//! Shared and classic group policy remains explicit at scenario boundaries.

use crate::{ConsumerId, GroupConsumerConfiguration, GroupOperationConfigMethod, GroupProtocol};

pub(super) fn validate(
    consumer_id: &ConsumerId,
    protocol: GroupProtocol,
    configuration: &Option<GroupConsumerConfiguration>,
    problems: &mut Vec<String>,
) {
    let Some(configuration) = configuration.as_ref() else {
        return;
    };
    let owner = format!("consumer {consumer_id}");
    crate::consumer_configuration::validate(
        &owner,
        configuration.fetch,
        configuration.limits,
        problems,
    );
    validate_runtime(consumer_id, configuration, problems);
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
    validate_durations(consumer_id, timings, problems);
}

fn validate_runtime(
    consumer_id: &ConsumerId,
    configuration: &GroupConsumerConfiguration,
    problems: &mut Vec<String>,
) {
    validate_durations(
        consumer_id,
        [
            ("processing_timeout_ms", configuration.processing_timeout_ms),
            (
                "membership_start_timeout_ms",
                configuration.membership_start_timeout_ms,
            ),
            ("seek_timeout_ms", configuration.seek_timeout_ms),
            ("close_timeout_ms", configuration.close_timeout_ms),
        ],
        problems,
    );
    if configuration.operation_config_method == GroupOperationConfigMethod::OperationConfig
        && (configuration.seek_timeout_ms.is_none() || configuration.close_timeout_ms.is_none())
    {
        problems.push(format!(
            "consumer {consumer_id} operation_config requires both seek_timeout_ms and close_timeout_ms"
        ));
    }
}

fn validate_durations<const N: usize>(
    consumer_id: &ConsumerId,
    fields: [(&str, Option<u64>); N],
    problems: &mut Vec<String>,
) {
    for (field, value) in fields {
        if value.is_some_and(|milliseconds| !(1..=i32::MAX as u64).contains(&milliseconds)) {
            problems.push(format!(
                "consumer {consumer_id} {field} must be between 1 and {}",
                i32::MAX
            ));
        }
    }
}
