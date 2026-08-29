//! Consumer-group protocol evidence distinguishes classic generations from KIP-848 epochs.

use serde::{Deserialize, Serialize};

/// Kafka consumer-group protocol selected through the packaged client.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupProtocol {
    /// `JoinGroup` and `SyncGroup` based classic membership.
    Classic,
    /// KIP-848 broker-managed consumer membership.
    Consumer,
}

/// Public policy used when a group has no committed offset.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupOffsetReset {
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

/// Portable group-consumer policy fixed before membership starts.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupConsumerConfiguration {
    /// Missing committed-offset behavior.
    pub offset_reset: GroupOffsetReset,
    /// Transactional record visibility.
    pub read_isolation: GroupReadIsolation,
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
