//! Portable classic-group timing maps only to Kafkars' public consumer builder policy.

use std::time::Duration;

use testlab_schema::GroupConsumerConfiguration;

use crate::kafkars_api::ClassicGroupConfig;

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
