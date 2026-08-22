//! Stable identifier types prevent accidental cross-correlation between domains.

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

const MAX_ID_BYTES: usize = 128;

/// A validation failure for one testlab identifier.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum IdError {
    /// The identifier was empty.
    #[error("identifier must not be empty")]
    Empty,
    /// The identifier exceeded the protocol bound.
    #[error("identifier exceeds {MAX_ID_BYTES} bytes")]
    TooLong,
    /// The first character was not ASCII alphanumeric.
    #[error("identifier must begin with an ASCII letter or digit")]
    InvalidStart,
    /// A later character was outside the portable alphabet.
    #[error("unsupported character {character:?} at byte {index}")]
    InvalidCharacter {
        /// Byte position in the original UTF-8 string.
        index: usize,
        /// Unsupported character.
        character: char,
    },
}

fn validate(value: &str) -> Result<(), IdError> {
    if value.is_empty() {
        return Err(IdError::Empty);
    }
    if value.len() > MAX_ID_BYTES {
        return Err(IdError::TooLong);
    }
    let mut characters = value.char_indices();
    let Some((_, first)) = characters.next() else {
        return Err(IdError::Empty);
    };
    if !first.is_ascii_alphanumeric() {
        return Err(IdError::InvalidStart);
    }
    for (index, character) in characters {
        let valid = character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | ':');
        if !valid {
            return Err(IdError::InvalidCharacter { index, character });
        }
    }
    Ok(())
}

macro_rules! identifier {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Creates a validated identifier.
            pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
                let value = value.into();
                validate(&value)?;
                Ok(Self(value))
            }

            /// Returns the portable string representation.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = IdError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

identifier!(AdapterId, "Stable identity for one adapter implementation.");
identifier!(ClientId, "Scenario-local identity for one client handle.");
identifier!(CommandId, "Correlation identity for one harness command.");
identifier!(ContractId, "Stable identity for one conformance contract.");
identifier!(
    EnvironmentId,
    "Stable identity for one independently controlled test environment."
);
identifier!(
    OperationId,
    "Scenario-local identity for one public operation."
);
identifier!(PackId, "Stable identity for one scenario pack.");
identifier!(
    ProducerId,
    "Scenario-local identity for one producer handle."
);
identifier!(RunId, "Unique identity for one testlab attempt.");
identifier!(ScenarioId, "Stable identity for one scenario definition.");
identifier!(StepId, "Stable identity for one ordered scenario step.");
identifier!(
    SubjectId,
    "Stable identity for one packaged subject definition."
);
