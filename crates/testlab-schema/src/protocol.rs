//! Versioned JSON Lines commands and events cross the adapter process boundary.

use serde::{Deserialize, Serialize};

use crate::{AdapterCommand, AdapterEvent, CommandId};

/// Current adapter control protocol version.
pub const PROTOCOL_VERSION: u16 = 34;

/// One correlated command sent from testctl to an adapter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommandEnvelope {
    /// Exact control protocol version.
    pub protocol_version: u16,
    /// Stable command correlation identity.
    pub command_id: CommandId,
    /// Command payload.
    #[serde(flatten)]
    pub command: AdapterCommand,
}

impl CommandEnvelope {
    /// Creates one protocol-v34 command envelope.
    pub fn new(command_id: CommandId, command: AdapterCommand) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            command_id,
            command,
        }
    }
}

/// One correlated event emitted by an adapter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AdapterEventEnvelope {
    /// Exact control protocol version.
    pub protocol_version: u16,
    /// Command that caused the event.
    pub command_id: CommandId,
    /// Event payload.
    #[serde(flatten)]
    pub event: AdapterEvent,
}

impl AdapterEventEnvelope {
    /// Creates one protocol-v34 event envelope.
    pub fn new(command_id: CommandId, event: AdapterEvent) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            command_id,
            event,
        }
    }
}
