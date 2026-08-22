//! Adapter connection policy and delivery certainty cross the process boundary.

use serde::{Deserialize, Serialize};

/// Environment variable carrying an ephemeral TLS certificate authority path.
pub const TLS_CA_PEM_ENVIRONMENT: &str = "TESTLAB_KAFKA_TLS_CA_PEM";
/// Environment variable carrying the ephemeral SASL username.
pub const SASL_USERNAME_ENVIRONMENT: &str = "TESTLAB_KAFKA_SASL_USERNAME";
/// Environment variable carrying the ephemeral SASL password.
pub const SASL_PASSWORD_ENVIRONMENT: &str = "TESTLAB_KAFKA_SASL_PASSWORD";

/// Adapter connection security without embedding secret values in history.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AdapterSecurity {
    /// Plain TCP without authentication.
    Plaintext,
    /// TLS with one environment-provided PEM trust bundle.
    TlsCustom {
        /// Name of the adapter environment variable containing the PEM path.
        ca_pem_environment: String,
    },
    /// SASL over plain TCP.
    SaslPlaintext {
        /// SASL mechanism.
        mechanism: AdapterSaslMechanism,
        /// Name of the adapter environment variable containing the username.
        username_environment: String,
        /// Name of the adapter environment variable containing the password.
        password_environment: String,
    },
    /// SASL over TLS.
    SaslTls {
        /// Name of the adapter environment variable containing the PEM path.
        ca_pem_environment: String,
        /// SASL mechanism.
        mechanism: AdapterSaslMechanism,
        /// Name of the adapter environment variable containing the username.
        username_environment: String,
        /// Name of the adapter environment variable containing the password.
        password_environment: String,
    },
}

/// SASL mechanism selected by an environment.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdapterSaslMechanism {
    /// SASL/PLAIN.
    Plain,
    /// SCRAM-SHA-256.
    ScramSha256,
    /// SCRAM-SHA-512.
    ScramSha512,
}

/// Normalized terminal delivery certainty.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalStatus {
    /// The client reports a broker acknowledgment.
    Acknowledged,
    /// The client knows the broker could not have accepted the operation.
    DefinitelyNotSent,
    /// The client cannot know whether the broker accepted the operation.
    PossiblySent,
}
