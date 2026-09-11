//! Release security composition stays explicit and independently loadable.

use std::path::Path;

use testlab_schema::{Authentication, EnvironmentDriver, TransportSecurity};

use crate::catalog::Repository;

#[test]
fn release_retains_every_sasl_mechanism_over_custom_root_tls() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = Repository::open(&root)
        .unwrap_or_else(|error| panic!("failed to open test repository: {error}"));
    let (_, qualification) = repository
        .load_qualification(Path::new("qualifications/kafkars-release.toml"))
        .unwrap_or_else(|error| panic!("load release qualification: {error}"));
    for (id, authentication) in [
        (
            "apache-kafka-4-3-1-three-sasl-plain-tls",
            Authentication::Plain,
        ),
        (
            "apache-kafka-4-3-1-three-scram-sha-256-tls",
            Authentication::ScramSha256,
        ),
        (
            "apache-kafka-4-3-1-three-scram-sha-512-tls",
            Authentication::ScramSha512,
        ),
    ] {
        let cell = qualification
            .cells
            .iter()
            .find(|cell| cell.id.as_str() == id)
            .unwrap_or_else(|| panic!("release qualification omitted {id}"));
        assert_eq!(cell.pack, "packs/kafkars-three-broker-security.toml");
        let (_, environment) = repository
            .load_environment(Path::new(&cell.environment))
            .unwrap_or_else(|error| panic!("load {id}: {error}"));
        let EnvironmentDriver::DockerCompose {
            security,
            compose_files,
            ..
        } = environment.driver
        else {
            panic!("{id} is not a compose environment");
        };
        assert_eq!(security.transport, TransportSecurity::TlsCustom);
        assert_eq!(security.authentication, authentication);
        assert!(
            compose_files
                .iter()
                .any(|path| path.ends_with("three-tls.yml"))
        );
        assert!(
            compose_files
                .iter()
                .any(|path| path.ends_with("three-sasl.yml"))
        );
    }
}
