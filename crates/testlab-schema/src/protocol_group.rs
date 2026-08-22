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
