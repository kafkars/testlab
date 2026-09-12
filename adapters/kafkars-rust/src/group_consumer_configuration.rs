//! Portable classic-group timing maps only to Kafkars' public consumer builder policy.

use std::time::Duration;

use testlab_schema::{GroupConsumerConfiguration, GroupOperationConfigMethod};

use crate::kafkars_api::{ClassicGroupConfig, ConsumerBuilder, GroupConsumerOperationConfig};
use crate::state::StateError;

pub(crate) fn apply_fetch_and_limits(
    mut builder: ConsumerBuilder,
    configuration: &GroupConsumerConfiguration,
) -> Result<ConsumerBuilder, StateError> {
    if let Some(fetch) = configuration.fetch {
        builder = builder.fetch_config(crate::consumer_configuration::public_fetch(fetch)?);
    }
    if let Some(limits) = configuration.limits {
        builder = builder.limits(crate::consumer_configuration::public_limits(limits)?);
    }
    Ok(builder)
}

pub(crate) fn apply_runtime_configuration(
    mut builder: ConsumerBuilder,
    configuration: &GroupConsumerConfiguration,
) -> Result<ConsumerBuilder, StateError> {
    if let Some(value) = configuration.processing_timeout_ms {
        builder = builder.processing_timeout(Duration::from_millis(value));
    }
    if let Some(value) = configuration.membership_start_timeout_ms {
        builder = builder.membership_start_timeout(Duration::from_millis(value));
    }
    match configuration.operation_config_method {
        GroupOperationConfigMethod::IndividualSetters => {
            if let Some(value) = configuration.seek_timeout_ms {
                builder = builder.seek_timeout(Duration::from_millis(value));
            }
            if let Some(value) = configuration.close_timeout_ms {
                builder = builder.close_timeout(Duration::from_millis(value));
            }
        }
        GroupOperationConfigMethod::OperationConfig => {
            let (Some(seek), Some(close)) = (
                configuration.seek_timeout_ms,
                configuration.close_timeout_ms,
            ) else {
                return Err(StateError::ConsumerConfiguration(
                    "operation_config requires both seek_timeout_ms and close_timeout_ms"
                        .to_owned(),
                ));
            };
            builder = builder.operation_config(GroupConsumerOperationConfig::new(
                Duration::from_millis(seek),
                Duration::from_millis(close),
            ));
        }
    }
    Ok(builder)
}

pub(crate) fn public_classic_group_config(
    configuration: &GroupConsumerConfiguration,
) -> Option<ClassicGroupConfig> {
    let session = configuration.classic_session_timeout_ms;
    let rebalance = configuration.classic_rebalance_timeout_ms;
    let heartbeat = configuration.classic_heartbeat_interval_ms;
    let heartbeat_attempt = configuration.classic_heartbeat_attempt_timeout_ms;
    let rejoin = configuration.classic_rejoin_backoff_ms;
    let rejoin_attempt = configuration.classic_rejoin_attempt_timeout_ms;
    let fields = [
        session,
        rebalance,
        heartbeat,
        heartbeat_attempt,
        rejoin,
        rejoin_attempt,
    ];
    if fields.iter().all(Option::is_none) {
        return None;
    }
    let mut selected = ClassicGroupConfig::default();
    if let Some(value) = session {
        selected = selected.with_session_timeout(Duration::from_millis(value));
    }
    if let Some(value) = rebalance {
        selected = selected.with_rebalance_timeout(Duration::from_millis(value));
    }
    if let Some(value) = heartbeat {
        selected = selected.with_heartbeat_interval(Duration::from_millis(value));
    }
    if let Some(value) = heartbeat_attempt {
        selected = selected.with_heartbeat_attempt_timeout(Duration::from_millis(value));
    }
    if let Some(value) = rejoin {
        selected = selected.with_rejoin_backoff(Duration::from_millis(value));
    }
    if let Some(value) = rejoin_attempt {
        selected = selected.with_rejoin_attempt_timeout(Duration::from_millis(value));
    }
    Some(selected)
}
