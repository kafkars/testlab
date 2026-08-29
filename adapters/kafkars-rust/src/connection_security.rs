//! Handshake security resolves ephemeral secrets without logging their values.

use std::env;
use std::fs;

use crate::kafkars_api::{Sasl, Security, Tls};
use testlab_schema::{AdapterSaslMechanism, AdapterSecurity};
use thiserror::Error;

pub(crate) fn resolve(configuration: AdapterSecurity) -> Result<Security, SecurityError> {
    resolve_with(configuration, |name| {
        env::var(name).map_err(|_| SecurityError::MissingEnvironment(name.to_owned()))
    })
}

pub(crate) fn resolve_with(
    configuration: AdapterSecurity,
    environment: impl Fn(&str) -> Result<String, SecurityError>,
) -> Result<Security, SecurityError> {
    match configuration {
        AdapterSecurity::Plaintext => Ok(Security::plaintext()),
        AdapterSecurity::TlsCustom { ca_pem_environment } => {
            let tls = tls(&environment(&ca_pem_environment)?)?;
            Ok(Security::tls(tls))
        }
        AdapterSecurity::SaslPlaintext {
            mechanism,
            username_environment,
            password_environment,
        } => Ok(Security::sasl_plaintext(sasl(
            mechanism,
            environment(&username_environment)?,
            environment(&password_environment)?,
        ))),
        AdapterSecurity::SaslTls {
            ca_pem_environment,
            mechanism,
            username_environment,
            password_environment,
        } => Ok(Security::sasl_tls(
            tls(&environment(&ca_pem_environment)?)?,
            sasl(
                mechanism,
                environment(&username_environment)?,
                environment(&password_environment)?,
            ),
        )),
    }
}

fn tls(path: &str) -> Result<Tls, SecurityError> {
    let pem = fs::read(path).map_err(|error| SecurityError::CertificateAuthority {
        path: path.to_owned(),
        error: error.to_string(),
    })?;
    Ok(Tls::custom_roots_pem(pem))
}

fn sasl(mechanism: AdapterSaslMechanism, username: String, password: String) -> Sasl {
    match mechanism {
        AdapterSaslMechanism::Plain => Sasl::plain(username, password),
        AdapterSaslMechanism::ScramSha256 => Sasl::scram_sha_256(username, password),
        AdapterSaslMechanism::ScramSha512 => Sasl::scram_sha_512(username, password),
    }
}

#[derive(Debug, Error)]
pub(crate) enum SecurityError {
    #[error("required security environment variable {0} is unavailable")]
    MissingEnvironment(String),
    #[error("failed to read TLS certificate authority {path}: {error}")]
    CertificateAuthority { path: String, error: String },
}
