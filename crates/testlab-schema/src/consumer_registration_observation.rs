//! Hosted consumer observations retain selected builder and returned-handle configuration.

use serde::{Deserialize, Serialize};

use crate::{
    ConsumerFetchConfiguration, ConsumerId, ConsumerLimitsConfiguration, GroupClassicAssignor,
    GroupConsumerConfiguration, GroupOffsetReset, GroupProtocol, GroupReadIsolation,
    ShareConsumerFetchConfiguration,
};

/// Caller-selected group policy read back through public consumer-builder views.
///
/// Optional values preserve whether the command explicitly selected that part
/// of the public policy rather than assigning meaning to implementation defaults.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupConsumerConfigurationSelection {
    /// Public `ConsumerBuilder::offset_reset` result.
    pub offset_reset: GroupOffsetReset,
    /// Public `ConsumerBuilder::selected_read_isolation` result.
    pub read_isolation: GroupReadIsolation,
    /// Explicit public Fetch policy, when selected by the command.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetch: Option<ConsumerFetchConfiguration>,
    /// Explicit public delivery-capacity policy, when selected by the command.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<ConsumerLimitsConfiguration>,
    /// Explicit public application-processing timeout in whole milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processing_timeout_ms: Option<u64>,
    /// Explicit public membership-start timeout in whole milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub membership_start_timeout_ms: Option<u64>,
    /// Explicit public seek timeout in whole milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seek_timeout_ms: Option<u64>,
    /// Explicit public close timeout in whole milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_timeout_ms: Option<u64>,
    /// Public `ConsumerBuilder::selected_group_instance_id` result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_instance_id: Option<String>,
    /// Explicit public classic assignor, when selected by the command.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classic_assignor: Option<GroupClassicAssignor>,
    /// Explicit classic session timeout in whole milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classic_session_timeout_ms: Option<u64>,
    /// Explicit classic rebalance timeout in whole milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classic_rebalance_timeout_ms: Option<u64>,
    /// Explicit classic heartbeat interval in whole milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classic_heartbeat_interval_ms: Option<u64>,
    /// Explicit classic heartbeat-attempt timeout in whole milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classic_heartbeat_attempt_timeout_ms: Option<u64>,
    /// Explicit classic rejoin backoff in whole milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classic_rejoin_backoff_ms: Option<u64>,
    /// Explicit classic rejoin-attempt timeout in whole milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classic_rejoin_attempt_timeout_ms: Option<u64>,
}

impl From<&GroupConsumerConfiguration> for GroupConsumerConfigurationSelection {
    fn from(value: &GroupConsumerConfiguration) -> Self {
        Self {
            offset_reset: value.offset_reset,
            read_isolation: value.read_isolation,
            fetch: value.fetch,
            limits: value.limits,
            processing_timeout_ms: value.processing_timeout_ms,
            membership_start_timeout_ms: value.membership_start_timeout_ms,
            seek_timeout_ms: value.seek_timeout_ms,
            close_timeout_ms: value.close_timeout_ms,
            group_instance_id: value.group_instance_id.clone(),
            classic_assignor: value.classic_assignor,
            classic_session_timeout_ms: value.classic_session_timeout_ms,
            classic_rebalance_timeout_ms: value.classic_rebalance_timeout_ms,
            classic_heartbeat_interval_ms: value.classic_heartbeat_interval_ms,
            classic_heartbeat_attempt_timeout_ms: value.classic_heartbeat_attempt_timeout_ms,
            classic_rejoin_backoff_ms: value.classic_rejoin_backoff_ms,
            classic_rejoin_attempt_timeout_ms: value.classic_rejoin_attempt_timeout_ms,
        }
    }
}

/// Exact values read from one successfully registered classic or KIP-848 consumer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupConsumerRegistrationObservation {
    /// Scenario-local handle identity carried by the registration command.
    pub consumer_id: ConsumerId,
    /// Public `Consumer::group_id` result.
    pub group_id: String,
    /// Public `Consumer::subscription` result in caller order.
    pub subscription: Vec<String>,
    /// Public `ConsumerBuilder::selected_group_protocol` result before registration.
    pub selected_protocol: GroupProtocol,
    /// Explicit configured values read through public builder views before registration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_configuration: Option<GroupConsumerConfigurationSelection>,
}

/// Exact values read from one successfully registered Share consumer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ShareConsumerRegistrationObservation {
    /// Scenario-local handle identity carried by the registration command.
    pub consumer_id: ConsumerId,
    /// Public `ShareConsumer::group_id` result.
    pub group_id: String,
    /// Public `ShareConsumer::subscription` result in caller order.
    pub subscription: Vec<String>,
    /// Public `ShareConsumer::rack` result.
    pub rack: Option<String>,
    /// Public builder values selected on the successful registration attempt.
    pub selected_builder: ShareConsumerBuilderSelection,
}

/// Exact public Share builder values retained for a successful registration attempt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ShareConsumerBuilderSelection {
    /// Public `ShareConsumerBuilder::selected_rack` result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rack: Option<String>,
    /// Explicit public Fetch policy, when selected by the command.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetch: Option<ShareConsumerFetchConfiguration>,
    /// Exact retry-adjusted public membership-start duration in nanoseconds.
    pub membership_start_timeout_ns: u64,
    /// Public selected close duration in whole milliseconds.
    pub close_timeout_ms: u64,
}

#[cfg(test)]
#[path = "consumer_registration_observation_test.rs"]
mod tests;
