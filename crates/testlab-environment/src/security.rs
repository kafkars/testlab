//! Environment-owned client security keeps secrets out of protocol history.

use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};

use rdkafka::ClientConfig;
use testlab_schema::{
    AdapterSaslMechanism, AdapterSecurity, Authentication, SASL_PASSWORD_ENVIRONMENT,
    SASL_USERNAME_ENVIRONMENT, SecurityProfile, TLS_CA_PEM_ENVIRONMENT, TransportSecurity,
};

use crate::ComposeFailure;

pub(super) const SASL_USERNAME: &str = "kafkars";
pub(super) const SASL_PASSWORD: &str = "kafkars-testlab-password";

#[derive(Clone)]
pub(super) struct ClientSecurity {
    profile: SecurityProfile,
    ca_pem: Option<String>,
}

impl ClientSecurity {
    pub(super) fn new(
        profile: SecurityProfile,
        ca_pem: Option<&Path>,
    ) -> Result<Self, ComposeFailure> {
        let ca_pem = ca_pem
            .map(|path| {
                path.to_str().map(str::to_owned).ok_or_else(|| {
                    ComposeFailure::new(
                        "environment_security_path_invalid",
                        format!("security path is not UTF-8: {}", path.display()),
                    )
                })
            })
            .transpose()?;
        if profile.transport == TransportSecurity::TlsCustom && ca_pem.is_none() {
            return Err(ComposeFailure::new(
                "environment_security_path_missing",
                "TLS custom trust requires a generated CA path",
            ));
        }
        Ok(Self { profile, ca_pem })
    }

    pub(super) fn external_protocol(&self) -> &'static str {
        match (self.profile.transport, self.profile.authentication) {
            (TransportSecurity::Plaintext, Authentication::None) => "PLAINTEXT",
            (TransportSecurity::TlsCustom, Authentication::None) => "SSL",
            (TransportSecurity::Plaintext, _) => "SASL_PLAINTEXT",
            (TransportSecurity::TlsCustom, _) => "SASL_SSL",
        }
    }

    pub(super) fn adapter_security(&self) -> AdapterSecurity {
        let mechanism = self.adapter_mechanism();
        match (self.profile.transport, mechanism) {
            (TransportSecurity::Plaintext, None) => AdapterSecurity::Plaintext,
            (TransportSecurity::TlsCustom, None) => AdapterSecurity::TlsCustom {
                ca_pem_environment: TLS_CA_PEM_ENVIRONMENT.to_owned(),
            },
            (TransportSecurity::Plaintext, Some(mechanism)) => AdapterSecurity::SaslPlaintext {
                mechanism,
                username_environment: SASL_USERNAME_ENVIRONMENT.to_owned(),
                password_environment: SASL_PASSWORD_ENVIRONMENT.to_owned(),
            },
            (TransportSecurity::TlsCustom, Some(mechanism)) => AdapterSecurity::SaslTls {
                ca_pem_environment: TLS_CA_PEM_ENVIRONMENT.to_owned(),
                mechanism,
                username_environment: SASL_USERNAME_ENVIRONMENT.to_owned(),
                password_environment: SASL_PASSWORD_ENVIRONMENT.to_owned(),
            },
        }
    }

    pub(super) fn adapter_environment(&self) -> Vec<(String, String)> {
        let mut environment = Vec::new();
        if let Some(ca_pem) = &self.ca_pem {
            environment.push((TLS_CA_PEM_ENVIRONMENT.to_owned(), ca_pem.clone()));
        }
        if self.profile.authentication != Authentication::None {
            environment.extend([
                (
                    SASL_USERNAME_ENVIRONMENT.to_owned(),
                    SASL_USERNAME.to_owned(),
                ),
                (
                    SASL_PASSWORD_ENVIRONMENT.to_owned(),
                    SASL_PASSWORD.to_owned(),
                ),
            ]);
        }
        environment
    }

    pub(super) fn compose_environment(
        &self,
        image: &str,
        host_port: u16,
        tls_directory: Option<&Path>,
    ) -> Vec<(String, String)> {
        let mut environment = vec![
            ("IMAGE".to_owned(), image.to_owned()),
            ("KAFKA_HOST_PORT".to_owned(), host_port.to_string()),
            (
                "KAFKA_EXTERNAL_PROTOCOL".to_owned(),
                self.external_protocol().to_owned(),
            ),
        ];
        if let Some(directory) = tls_directory {
            environment.push(("KAFKA_TLS_DIR".to_owned(), directory.display().to_string()));
        }
        if self.scram_mechanism().is_some() {
            environment.push((
                "TESTLAB_SCRAM_PASSWORD".to_owned(),
                SASL_PASSWORD.to_owned(),
            ));
        }
        environment
    }

    pub(super) fn configure(&self, config: &mut ClientConfig) {
        config.set("security.protocol", self.external_protocol());
        if let Some(ca_pem) = &self.ca_pem {
            config.set("ssl.ca.location", ca_pem);
        }
        if self.profile.authentication != Authentication::None {
            config
                .set("sasl.mechanism", self.librdkafka_mechanism())
                .set("sasl.username", SASL_USERNAME)
                .set("sasl.password", SASL_PASSWORD);
        }
    }

    fn adapter_mechanism(&self) -> Option<AdapterSaslMechanism> {
        match self.profile.authentication {
            Authentication::None => None,
            Authentication::Plain => Some(AdapterSaslMechanism::Plain),
            Authentication::ScramSha256 => Some(AdapterSaslMechanism::ScramSha256),
            Authentication::ScramSha512 => Some(AdapterSaslMechanism::ScramSha512),
        }
    }

    fn librdkafka_mechanism(&self) -> &'static str {
        match self.profile.authentication {
            Authentication::None => "",
            Authentication::Plain => "PLAIN",
            Authentication::ScramSha256 => "SCRAM-SHA-256",
            Authentication::ScramSha512 => "SCRAM-SHA-512",
        }
    }

    pub(super) fn scram_mechanism(&self) -> Option<&'static str> {
        match self.profile.authentication {
            Authentication::ScramSha256 => Some("SCRAM-SHA-256"),
            Authentication::ScramSha512 => Some("SCRAM-SHA-512"),
            Authentication::None | Authentication::Plain => None,
        }
    }

    pub(super) fn ca_pem_path(&self) -> Option<PathBuf> {
        self.ca_pem.as_deref().map(PathBuf::from)
    }
}

impl Debug for ClientSecurity {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientSecurity")
            .field("profile", &self.profile)
            .field("ca_pem", &self.ca_pem)
            .finish_non_exhaustive()
    }
}
