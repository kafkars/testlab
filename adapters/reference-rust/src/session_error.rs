//! Reference adapter failures carry stable protocol codes without leaking state.

use thiserror::Error;

use crate::state::StateError;

const MAX_COMMAND_BYTES: usize = 4 * 1024 * 1024;

/// Reference adapter protocol or lifecycle failure.
#[derive(Debug, Error)]
pub enum AdapterError {
    /// Standard stream or model transport I/O failed.
    #[error("adapter I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// One control message was malformed.
    #[error("adapter JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    /// One stable identifier was invalid.
    #[error("adapter identity failed: {0}")]
    Id(#[from] testlab_schema::IdError),
    /// Public fixture lifecycle was invalid.
    #[error("adapter state failed: {0}")]
    State(String),
    /// One batch command did not contain an operation.
    #[error("adapter batch failed: {0}")]
    Batch(String),
    /// A command reached a capability this adapter does not declare.
    #[error("unsupported adapter command: {0}")]
    Unsupported(&'static str),
    /// The harness used an unsupported protocol version.
    #[error("unsupported protocol version {0}")]
    ProtocolVersion(u16),
    /// A command exceeded the bounded input size.
    #[error("command exceeded {MAX_COMMAND_BYTES} bytes")]
    CommandTooLarge,
    /// One command ended without its JSON Lines delimiter.
    #[error("command ended without a newline delimiter")]
    IncompleteCommand,
    /// The harness closed stdin before `finish` settled.
    #[error("stdin closed before finish")]
    UnexpectedEof,
}

impl AdapterError {
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Io(_) => "adapter_io",
            Self::Json(_) => "adapter_json",
            Self::Id(_) => "adapter_identity",
            Self::State(_) => "adapter_state",
            Self::Batch(_) => "adapter_batch",
            Self::Unsupported(_) => "adapter_unsupported",
            Self::ProtocolVersion(_) => "protocol_version",
            Self::CommandTooLarge => "command_too_large",
            Self::IncompleteCommand => "incomplete_command",
            Self::UnexpectedEof => "unexpected_eof",
        }
    }
}

impl From<StateError> for AdapterError {
    fn from(error: StateError) -> Self {
        Self::State(error.to_string())
    }
}
