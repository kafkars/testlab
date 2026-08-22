//! Security tests prove complete configuration without exposing credentials.

use std::path::Path;

use testlab_schema::{
    AdapterSaslMechanism, AdapterSecurity, Authentication, SecurityProfile, TransportSecurity,
};

use crate::security::{ClientSecurity, SASL_PASSWORD};

#[test]
fn plaintext_has_no_secret_environment() {
    let security = must(ClientSecurity::new(profile(Authentication::None), None));

    assert_eq!(security.external_protocol(), "PLAINTEXT");
    assert_eq!(security.adapter_security(), AdapterSecurity::Plaintext);
    assert!(security.adapter_environment().is_empty());
}

#[test]
fn scram_references_environment_without_embedding_password() {
    let security = must(ClientSecurity::new(
        profile(Authentication::ScramSha512),
        None,
    ));

    assert!(matches!(
        security.adapter_security(),
        AdapterSecurity::SaslPlaintext {
            mechanism: AdapterSaslMechanism::ScramSha512,
            ..
        }
    ));
    assert!(!format!("{security:?}").contains(SASL_PASSWORD));
    assert_eq!(security.adapter_environment().len(), 2);
}

#[test]
fn tls_requires_and_references_a_ca_path() {
    let missing = ClientSecurity::new(
        SecurityProfile {
            transport: TransportSecurity::TlsCustom,
            authentication: Authentication::None,
        },
        None,
    );
    assert!(missing.is_err());

    let security = must(ClientSecurity::new(
        SecurityProfile {
            transport: TransportSecurity::TlsCustom,
            authentication: Authentication::None,
        },
        Some(Path::new("/tmp/testlab/ca.pem")),
    ));
    assert!(matches!(
        security.adapter_security(),
        AdapterSecurity::TlsCustom { .. }
    ));
}

fn profile(authentication: Authentication) -> SecurityProfile {
    SecurityProfile {
        transport: TransportSecurity::Plaintext,
        authentication,
    }
}

fn must<T, E: std::fmt::Display>(result: Result<T, E>) -> T {
    result.unwrap_or_else(|error| panic!("create security: {error}"))
}
