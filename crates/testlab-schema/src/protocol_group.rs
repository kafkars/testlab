//! Consumer-group protocol evidence distinguishes classic generations from KIP-848 epochs.

#[cfg(test)]
#[path = "group_operation_config_validation_test.rs"]
mod operation_config_validation_tests;

#[cfg(test)]
#[path = "group_checkpoint_method_test.rs"]
mod checkpoint_method_tests;

use serde::{Deserialize, Serialize};

/// Stable public client error emitted when a group has no committed offset.
pub const GROUP_MISSING_OFFSET_ERROR_CODE: &str = "state";

/// Kafka consumer-group protocol selected through the packaged client.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupProtocol {
    /// `JoinGroup` and `SyncGroup` based classic membership.
    Classic,
    /// KIP-848 broker-managed consumer membership.
    Consumer,
}

/// Public batch observation selected for one hosted group receive.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupConsumerReceiveMethod {
    /// Wait on the named retained-delivery observer.
    #[default]
    Recv,
    /// Repeatedly attempt the immediate retained-batch take operation.
    TryTakeBatch,
}

/// Public full-batch checkpoint conversion selected after one hosted receive.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupCheckpointMethod {
    /// Convert through the canonical `checkpoint` method.
    #[default]
    Checkpoint,
    /// Convert through the compatibility `into_checkpoint` method.
    IntoCheckpoint,
}

/// Public policy used when a group has no committed offset.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupOffsetReset {
    /// Fail instead of guessing a position.
    Error,
    /// Begin at the earliest available offset.
    #[default]
    Earliest,
    /// Begin after the latest available offset.
    Latest,
}

/// Public transactional visibility selected for group records.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupReadIsolation {
    /// Deliver committed, aborted, and unresolved transaction records.
    #[default]
    ReadUncommitted,
    /// Deliver only nontransactional and committed transaction records.
    ReadCommitted,
}

/// Public builder surface selected for hosted-group seek and close durations.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupOperationConfigMethod {
    /// Apply each duration through its dedicated builder setter.
    #[default]
    IndividualSetters,
    /// Apply both durations through one `GroupConsumerOperationConfig` value.
    OperationConfig,
}

/// Public partition assignor selected for classic group membership.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupClassicAssignor {
    /// Assign each topic's partitions in contiguous ranges.
    #[default]
    Range,
    /// Retain prior ownership where possible through cooperative rebalances.
    CooperativeSticky,
}

/// Portable group-consumer policy fixed before membership starts.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupConsumerConfiguration {
    /// Missing committed-offset behavior.
    pub offset_reset: GroupOffsetReset,
    /// Transactional record visibility.
    pub read_isolation: GroupReadIsolation,
    /// Optional broker Fetch policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetch: Option<crate::ConsumerFetchConfiguration>,
    /// Optional Fetch-call and retained-delivery capacities.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<crate::ConsumerLimitsConfiguration>,
    /// Optional maximum application-processing interval apart from membership work.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processing_timeout_ms: Option<u64>,
    /// Optional end-to-end timeout for the first successful membership operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub membership_start_timeout_ms: Option<u64>,
    /// Optional end-to-end timeout for each later group seek.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seek_timeout_ms: Option<u64>,
    /// Optional end-to-end timeout for explicit or requested group close.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_timeout_ms: Option<u64>,
    /// Exact public builder surface used for seek and close durations.
    #[serde(default)]
    pub operation_config_method: GroupOperationConfigMethod,
    /// Optional stable broker-visible member identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_instance_id: Option<String>,
    /// Optional classic-group partition assignor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classic_assignor: Option<GroupClassicAssignor>,
    /// Optional classic-group session timeout sent with each Join request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classic_session_timeout_ms: Option<u64>,
    /// Optional classic-group rebalance timeout sent with each Join request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classic_rebalance_timeout_ms: Option<u64>,
    /// Optional delay between successful classic-group heartbeats.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classic_heartbeat_interval_ms: Option<u64>,
    /// Optional end-to-end timeout for one classic-group heartbeat attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classic_heartbeat_attempt_timeout_ms: Option<u64>,
    /// Optional delay before a recoverable classic-group rejoin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classic_rejoin_backoff_ms: Option<u64>,
    /// Optional end-to-end timeout for one internally retried Join cycle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classic_rejoin_attempt_timeout_ms: Option<u64>,
}

/// Exact public group-consumer owner abandoned without member leave.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupConsumerAbandonment {
    /// Scenario-local consumer identity.
    pub consumer_id: crate::ConsumerId,
}

/// Public membership epoch observed after a group receive and commit.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GroupMembershipEpoch {
    /// Classic group generation.
    Classic {
        /// Positive broker-assigned generation identity.
        generation_id: i32,
    },
    /// KIP-848 consumer group epoch.
    Consumer {
        /// Positive broker-assigned member epoch.
        member_epoch: i32,
    },
}

impl GroupMembershipEpoch {
    /// Returns the protocol family proven by this public epoch.
    #[must_use]
    pub const fn protocol(self) -> GroupProtocol {
        match self {
            Self::Classic { .. } => GroupProtocol::Classic,
            Self::Consumer { .. } => GroupProtocol::Consumer,
        }
    }

    /// Returns whether the broker assigned a live positive epoch.
    #[must_use]
    pub const fn is_positive(self) -> bool {
        match self {
            Self::Classic { generation_id } => generation_id > 0,
            Self::Consumer { member_epoch } => member_epoch > 0,
        }
    }
}
