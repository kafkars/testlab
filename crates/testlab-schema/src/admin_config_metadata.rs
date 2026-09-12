//! Public metadata returned for one described Kafka configuration entry.

use serde::{Deserialize, Serialize};

/// One Kafka configuration synonym returned by the public client.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConfigSynonym {
    /// Exact synonym name.
    pub name: String,
    /// Nullable synonym value, preserving sensitive or unavailable values.
    pub value: Option<String>,
    /// Kafka's exact signed configuration-source value.
    pub source: i8,
}

/// Complete public metadata for one successfully described configuration entry.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConfigEntryMetadata {
    /// Whether Kafka marks the configuration read-only.
    pub read_only: bool,
    /// Kafka's exact signed configuration-source value.
    pub source: i8,
    /// Whether Kafka marks the configuration sensitive.
    pub sensitive: bool,
    /// Caller-requested configuration synonyms in public normalized order.
    pub synonyms: Vec<AdminConfigSynonym>,
    /// Kafka's signed configuration-type value when supplied.
    pub config_type: Option<i8>,
    /// Kafka's nullable configuration documentation when supplied.
    pub documentation: Option<String>,
}
