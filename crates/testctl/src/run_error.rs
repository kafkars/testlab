//! Runner errors separate hard repository failures from sealed invalid attempts.

use std::path::PathBuf;

use testlab_schema::{ContractId, HarnessError, Violation};
use thiserror::Error;

/// A hard CLI, catalog, or evidence error that cannot be represented as a run.
#[derive(Debug, Error)]
pub enum AppError {
    /// Filesystem work failed with contextual ownership.
    #[error("{context}: {source}")]
    Io {
        /// Operation that failed.
        context: String,
        /// Underlying operating-system error.
        #[source]
        source: std::io::Error,
    },
    /// A TOML manifest was malformed.
    #[error("failed to parse TOML manifest {path}: {source}")]
    Toml {
        /// Manifest path.
        path: PathBuf,
        /// Parser failure.
        #[source]
        source: toml::de::Error,
    },
    /// A JSON evidence value could not be serialized.
    #[error("failed to serialize {context}: {source}")]
    Json {
        /// Artifact being serialized.
        context: String,
        /// Serializer failure.
        #[source]
        source: serde_json::Error,
    },
    /// A repository manifest or path violated a static contract.
    #[error("catalog validation failed: {0}")]
    Catalog(String),
    /// A sealed run could not be completed safely.
    #[error("evidence sealing failed: {0}")]
    Evidence(String),
}

impl AppError {
    pub(crate) fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }

    pub(crate) fn json(context: impl Into<String>, source: serde_json::Error) -> Self {
        Self::Json {
            context: context.into(),
            source,
        }
    }
}

#[derive(Clone, Debug, Error)]
#[error("{code}: {diagnostic}")]
pub(crate) struct RunFailure {
    contract: &'static str,
    code: String,
    diagnostic: String,
    evidence: Vec<String>,
}

impl RunFailure {
    fn new(contract: &'static str, code: impl Into<String>, diagnostic: impl Into<String>) -> Self {
        Self {
            contract,
            code: code.into(),
            diagnostic: diagnostic.into(),
            evidence: Vec::new(),
        }
    }

    pub(crate) fn harness(code: impl Into<String>, diagnostic: impl Into<String>) -> Self {
        Self::new("HARNESS-001", code, diagnostic)
    }

    pub(crate) fn protocol(code: impl Into<String>, diagnostic: impl Into<String>) -> Self {
        Self::new("PROTO-001", code, diagnostic)
    }

    pub(crate) fn capability(diagnostic: impl Into<String>) -> Self {
        Self::new("CAP-001", "missing_capability", diagnostic)
    }

    pub(crate) fn harness_error(&self) -> HarnessError {
        HarnessError {
            code: self.code.clone(),
            diagnostic: bounded(&self.diagnostic, 4096),
        }
    }

    pub(crate) fn violation(&self) -> Violation {
        let contract_id = match ContractId::new(self.contract) {
            Ok(contract_id) => contract_id,
            Err(error) => panic!("invalid internal contract id: {error}"),
        };
        Violation {
            contract_id,
            message: bounded(&self.diagnostic, 4096),
            operation_id: None,
            evidence: self.evidence.clone(),
        }
    }
}

fn bounded(value: &str, maximum: usize) -> String {
    if value.len() <= maximum {
        return value.to_owned();
    }
    let mut end = maximum;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &value[..end])
}
