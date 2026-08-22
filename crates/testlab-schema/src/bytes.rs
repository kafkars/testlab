//! Explicit byte encodings preserve null, empty, text, and binary distinctions.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Encoding used by one byte string in a manifest.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ByteEncoding {
    /// UTF-8 text encoded exactly as written.
    Utf8,
    /// Lowercase or uppercase hexadecimal bytes.
    Hex,
}

/// Portable byte input for TOML and JSON contracts.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ByteString {
    /// Encoding of `data`.
    pub encoding: ByteEncoding,
    /// Encoded data.
    pub data: String,
}

impl ByteString {
    /// Creates a UTF-8 byte string.
    pub fn utf8(value: impl Into<String>) -> Self {
        Self {
            encoding: ByteEncoding::Utf8,
            data: value.into(),
        }
    }

    /// Encodes exact bytes as lowercase hexadecimal.
    pub fn hex(value: impl AsRef<[u8]>) -> Self {
        Self {
            encoding: ByteEncoding::Hex,
            data: hex::encode(value),
        }
    }

    /// Decodes the portable representation.
    pub fn decode(&self) -> Result<Vec<u8>, ByteStringError> {
        match self.encoding {
            ByteEncoding::Utf8 => Ok(self.data.as_bytes().to_vec()),
            ByteEncoding::Hex => hex::decode(&self.data).map_err(ByteStringError::InvalidHex),
        }
    }
}

/// Failure while decoding one portable byte value.
#[derive(Debug, Error)]
pub enum ByteStringError {
    /// The hexadecimal representation was malformed.
    #[error("invalid hexadecimal byte string: {0}")]
    InvalidHex(#[from] hex::FromHexError),
}
