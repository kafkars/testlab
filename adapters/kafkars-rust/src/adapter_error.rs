//! Adapter failures retain stable protocol codes and bounded public diagnostics.

use thiserror::Error;

use crate::state::StateError;

/// Kafkars adapter protocol, conversion, or public-client failure.
#[derive(Debug, Error)]
pub enum AdapterError {
    /// Standard stream I/O failed.
    #[error("adapter I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// One control message was malformed.
    #[error("adapter JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    /// One stable identifier was invalid.
    #[error("adapter identity failed: {0}")]
    Id(#[from] testlab_schema::IdError),
    /// One portable byte value could not be decoded.
    #[error("adapter record bytes failed: {0}")]
    Bytes(#[from] testlab_schema::ByteStringError),
    /// Public adapter lifecycle was invalid.
    #[error("adapter state failed: {0}")]
    State(String),
    /// A packaged public Kafkars operation failed.
    #[error("packaged Kafkars operation failed: {0}")]
    Client(kafkars::KafkaError),
    /// The harness used an unsupported protocol version.
    #[error("unsupported protocol version {0}")]
    ProtocolVersion(u16),
    /// A command exceeded the bounded input size.
    #[error("command exceeded 4194304 bytes")]
    CommandTooLarge,
    /// A command lacked its JSON Lines delimiter.
    #[error("command ended without a newline delimiter")]
    IncompleteCommand,
    /// The harness closed stdin before finish.
    #[error("stdin closed before finish")]
    UnexpectedEof,
}

impl From<StateError> for AdapterError {
    fn from(error: StateError) -> Self {
        match error {
            StateError::Client(error) => Self::Client(error),
            other => Self::State(other.to_string()),
        }
    }
}

impl AdapterError {
    pub(crate) const fn client_failure(&self) -> Option<&kafkars::KafkaError> {
        match self {
            Self::Client(error) => Some(error),
            _ => None,
        }
    }

    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Io(_) => "adapter_io",
            Self::Json(_) => "adapter_json",
            Self::Id(_) => "adapter_identity",
            Self::Bytes(_) => "adapter_record_bytes",
            Self::State(_) => "adapter_state",
            Self::Client(_) => "kafkars_operation",
            Self::ProtocolVersion(_) => "protocol_version",
            Self::CommandTooLarge => "command_too_large",
            Self::IncompleteCommand => "incomplete_command",
            Self::UnexpectedEof => "unexpected_eof",
        }
    }
}
