//! Portable group configuration maps to and from Kafkars' public consumer-builder policy.

use std::time::Duration;

use testlab_schema::{
    GroupClassicAssignor, GroupConsumerConfiguration, GroupConsumerConfigurationSelection,
    GroupOffsetReset, GroupOperationConfigMethod, GroupProtocol, GroupReadIsolation,
};

use crate::kafkars_api::{
    ClassicGroupAssignor, ClassicGroupConfig, ConsumerBuilder, ConsumerGroupProtocol,
    GroupConsumerOperationConfig, OffsetReset, ReadIsolation,
};
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

pub(crate) fn selected(
    builder: &ConsumerBuilder,
    requested: Option<&GroupConsumerConfiguration>,
) -> Result<(GroupProtocol, Option<GroupConsumerConfigurationSelection>), StateError> {
    let protocol = portable_protocol(builder.selected_group_protocol());
    let Some(requested) = requested else {
        return Ok((protocol, None));
    };
    let operation = builder.selected_operation_config();
    let seek_timeout = builder.selected_seek_timeout();
    let close_timeout = builder.selected_close_timeout();
    if operation.seek_timeout() != seek_timeout || operation.close_timeout() != close_timeout {
        return Err(selected_invalid(
            "public group operation-policy getters disagreed",
        ));
    }
    let fetch = builder.selected_fetch_config();
    let limits = builder.selected_limits();
    let classic = builder.selected_classic_group_config();
    let assignor = builder.selected_classic_group_assignor();
    Ok((
        protocol,
        Some(GroupConsumerConfigurationSelection {
            offset_reset: portable_offset_reset(builder.offset_reset()),
            read_isolation: portable_read_isolation(builder.selected_read_isolation()),
            fetch: requested
                .fetch
                .is_some()
                .then(|| crate::consumer_configuration::selected_fetch(fetch))
                .transpose()?,
            limits: requested
                .limits
                .is_some()
                .then(|| crate::consumer_configuration::selected_limits(limits))
                .transpose()?,
            processing_timeout_ms: selected_optional_millis(
                requested.processing_timeout_ms,
                builder.selected_processing_timeout(),
                "processing_timeout",
            )?,
            membership_start_timeout_ms: selected_optional_millis(
                requested.membership_start_timeout_ms,
                builder.selected_membership_start_timeout(),
                "membership_start_timeout",
            )?,
            seek_timeout_ms: selected_optional_millis(
                requested.seek_timeout_ms,
                seek_timeout,
                "seek_timeout",
            )?,
            close_timeout_ms: selected_optional_millis(
                requested.close_timeout_ms,
                close_timeout,
                "close_timeout",
            )?,
            group_instance_id: builder.selected_group_instance_id().map(str::to_owned),
            classic_assignor: selected_assignor(requested.classic_assignor, assignor)?,
            classic_session_timeout_ms: selected_optional_millis(
                requested.classic_session_timeout_ms,
                classic.session_timeout(),
                "classic_session_timeout",
            )?,
            classic_rebalance_timeout_ms: selected_optional_millis(
                requested.classic_rebalance_timeout_ms,
                classic.rebalance_timeout(),
                "classic_rebalance_timeout",
            )?,
            classic_heartbeat_interval_ms: selected_optional_millis(
                requested.classic_heartbeat_interval_ms,
                classic.heartbeat_interval(),
                "classic_heartbeat_interval",
            )?,
            classic_heartbeat_attempt_timeout_ms: selected_optional_millis(
                requested.classic_heartbeat_attempt_timeout_ms,
                classic.heartbeat_attempt_timeout(),
                "classic_heartbeat_attempt_timeout",
            )?,
            classic_rejoin_backoff_ms: selected_optional_millis(
                requested.classic_rejoin_backoff_ms,
                classic.rejoin_backoff(),
                "classic_rejoin_backoff",
            )?,
            classic_rejoin_attempt_timeout_ms: selected_optional_millis(
                requested.classic_rejoin_attempt_timeout_ms,
                classic.rejoin_attempt_timeout(),
                "classic_rejoin_attempt_timeout",
            )?,
        }),
    ))
}

fn selected_optional_millis(
    requested: Option<u64>,
    selected: Duration,
    field: &str,
) -> Result<Option<u64>, StateError> {
    requested
        .map(|_| crate::consumer_configuration::whole_millis(selected, field))
        .transpose()
}

fn selected_assignor(
    requested: Option<GroupClassicAssignor>,
    selected: Option<ClassicGroupAssignor>,
) -> Result<Option<GroupClassicAssignor>, StateError> {
    match requested {
        None => Ok(None),
        Some(_) => selected
            .map(portable_assignor)
            .map(Some)
            .ok_or_else(|| selected_invalid("explicit classic assignor was not selected")),
    }
}

const fn portable_protocol(value: ConsumerGroupProtocol) -> GroupProtocol {
    match value {
        ConsumerGroupProtocol::Classic => GroupProtocol::Classic,
        ConsumerGroupProtocol::Consumer => GroupProtocol::Consumer,
    }
}

const fn portable_offset_reset(value: OffsetReset) -> GroupOffsetReset {
    match value {
        OffsetReset::Error => GroupOffsetReset::Error,
        OffsetReset::Earliest => GroupOffsetReset::Earliest,
        OffsetReset::Latest => GroupOffsetReset::Latest,
    }
}

const fn portable_read_isolation(value: ReadIsolation) -> GroupReadIsolation {
    match value {
        ReadIsolation::ReadUncommitted => GroupReadIsolation::ReadUncommitted,
        ReadIsolation::ReadCommitted => GroupReadIsolation::ReadCommitted,
    }
}

const fn portable_assignor(value: ClassicGroupAssignor) -> GroupClassicAssignor {
    match value {
        ClassicGroupAssignor::Range => GroupClassicAssignor::Range,
        ClassicGroupAssignor::CooperativeSticky => GroupClassicAssignor::CooperativeSticky,
    }
}

fn selected_invalid(message: &str) -> StateError {
    StateError::ConsumerConfiguration(message.to_owned())
}
