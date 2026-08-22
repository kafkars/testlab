//! Machine-readable conformance contracts give every violation a stable identity.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::ContractId;

/// Repository conformance registry.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContractRegistry {
    /// Exact registry schema version.
    pub schema: u16,
    /// Ordered contract definitions.
    #[serde(rename = "contract")]
    pub contracts: Vec<ContractDefinition>,
}

impl ContractRegistry {
    /// Validates stable unique contract identities.
    pub fn validate(&self) -> Result<(), ContractRegistryError> {
        if self.schema != 1 {
            return Err(ContractRegistryError::UnsupportedVersion(self.schema));
        }
        let mut identifiers = BTreeSet::new();
        for contract in &self.contracts {
            if contract.title.trim().is_empty()
                || contract.statement.trim().is_empty()
                || contract.category.trim().is_empty()
            {
                return Err(ContractRegistryError::EmptyText(contract.id.clone()));
            }
            if !identifiers.insert(contract.id.clone()) {
                return Err(ContractRegistryError::DuplicateId(contract.id.clone()));
            }
        }
        Ok(())
    }
}

/// One named black-box contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContractDefinition {
    /// Stable contract identity.
    pub id: ContractId,
    /// Short human-readable title.
    pub title: String,
    /// Exact semantic rule.
    pub statement: String,
    /// Broad ownership category.
    pub category: String,
}

/// Invalid conformance registry.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ContractRegistryError {
    /// The registry schema is unknown.
    #[error("unsupported contract registry schema version {0}")]
    UnsupportedVersion(u16),
    /// One identity appeared more than once.
    #[error("duplicate contract id {0}")]
    DuplicateId(ContractId),
    /// A contract lacked reviewable text.
    #[error("contract {0} has empty title, statement, or category")]
    EmptyText(ContractId),
}
