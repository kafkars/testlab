//! Connection security tests prove secret references and redacted diagnostics.

use testlab_schema::{AdapterSaslMechanism, AdapterSecurity};

use super::connection_security::{SecurityError, resolve_with};

#[test]
fn plaintext_needs_no_environment() {
    let security = resolve_with(AdapterSecurity::Plaintext, |_| {
        Err(SecurityError::MissingEnvironment("unexpected".to_owned()))
    })
    .unwrap_or_else(|error| panic!("resolve plaintext: {error}"));

    assert_eq!(format!("{security:?}"), "Plaintext");
}

#[test]
fn sasl_password_is_not_exposed_by_diagnostics() {
    let password = "qualification-secret";
    let security = resolve_with(
        AdapterSecurity::SaslPlaintext {
            mechanism: AdapterSaslMechanism::ScramSha256,
            username_environment: "USERNAME".to_owned(),
            password_environment: "PASSWORD".to_owned(),
        },
        |name| match name {
            "USERNAME" => Ok("kafkars".to_owned()),
            "PASSWORD" => Ok(password.to_owned()),
            other => Err(SecurityError::MissingEnvironment(other.to_owned())),
        },
    )
    .unwrap_or_else(|error| panic!("resolve SASL: {error}"));

    assert!(!format!("{security:?}").contains(password));
}
