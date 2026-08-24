//! Shared Compose test fixtures retain fake terminal behavior outside behavioral tests.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use testlab_schema::{
    Authentication, BrokerIdentity, EnvironmentDriver, EnvironmentId, EnvironmentManifest, RunId,
    SecurityProfile, TransportSecurity,
};

use crate::{ComposeRequest, DockerComposeEnvironment};

static FIXTURE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(super) struct Fixture {
    root: PathBuf,
    program: PathBuf,
    manifest: EnvironmentManifest,
    run_id: RunId,
}

impl Fixture {
    pub(super) fn new(fail_up: bool) -> Self {
        Self::with_behavior(fail_up, false, false, Authentication::None)
    }

    pub(super) fn with_authentication(fail_up: bool, authentication: Authentication) -> Self {
        Self::with_behavior(fail_up, false, false, authentication)
    }

    pub(super) fn with_startup_exit(persistent: bool) -> Self {
        Self::with_behavior(false, true, persistent, Authentication::None)
    }

    fn with_behavior(
        fail_up: bool,
        startup_exit: bool,
        persistent_startup_exit: bool,
        authentication: Authentication,
    ) -> Self {
        Self::with_security_behavior(
            fail_up,
            startup_exit,
            persistent_startup_exit,
            SecurityProfile {
                transport: TransportSecurity::Plaintext,
                authentication,
            },
        )
    }

    pub(super) fn with_security(fail_up: bool, security: SecurityProfile) -> Self {
        Self::with_security_behavior(fail_up, false, false, security)
    }

    fn with_security_behavior(
        fail_up: bool,
        startup_exit: bool,
        persistent_startup_exit: bool,
        security: SecurityProfile,
    ) -> Self {
        let sequence = FIXTURE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("testlab-compose-{}-{sequence}", std::process::id()));
        fs::create_dir(&root)
            .unwrap_or_else(|error| panic!("create fixture {}: {error}", root.display()));
        let program = root.join("fake-docker");
        fs::write(
            &program,
            fake_docker(fail_up, startup_exit, persistent_startup_exit),
        )
        .unwrap_or_else(|error| panic!("write fake Docker program: {error}"));
        make_executable(&program);
        Self {
            root,
            program,
            manifest: manifest(security),
            run_id: RunId::new(format!("run-compose-{sequence}"))
                .unwrap_or_else(|error| panic!("fixture run id: {error}")),
        }
    }

    pub(super) fn with_feature_level(name: &str, level: u16) -> Self {
        let mut fixture = Self::with_authentication(false, Authentication::ScramSha256);
        let EnvironmentDriver::DockerCompose { feature_levels, .. } = &mut fixture.manifest.driver
        else {
            panic!("fixture must use Docker Compose");
        };
        feature_levels.insert(name.to_owned(), level);
        fixture
    }

    pub(super) fn environment(&self) -> DockerComposeEnvironment {
        DockerComposeEnvironment::new_with_program(
            ComposeRequest {
                repository_root: &self.root,
                environment: &self.manifest,
                run_id: &self.run_id,
                started_unix_ms: 1,
            },
            self.program.clone(),
            29092,
        )
        .unwrap_or_else(|error| panic!("create Compose environment: {error}"))
    }

    pub(super) fn log(&self) -> String {
        fs::read_to_string(self.program.with_extension("log"))
            .unwrap_or_else(|error| panic!("read fake Docker log: {error}"))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn manifest(security: SecurityProfile) -> EnvironmentManifest {
    EnvironmentManifest {
        schema_version: 2,
        id: EnvironmentId::new("apache-kafka-test")
            .unwrap_or_else(|error| panic!("fixture environment id: {error}")),
        title: "Apache Kafka test fixture".to_owned(),
        driver: EnvironmentDriver::DockerCompose {
            broker: BrokerIdentity {
                implementation: "apache-kafka".to_owned(),
                version: "4.3.1".to_owned(),
            },
            image: format!("apache/kafka@sha256:{}", "a".repeat(64)),
            cluster_size: 1,
            security,
            compose_files: vec!["clusters/kafka.yml".to_owned()],
            broker_services: vec!["broker".to_owned()],
            client_port: 9092,
            feature_levels: BTreeMap::new(),
        },
    }
}

fn fake_docker(fail_up: bool, startup_exit: bool, persistent_startup_exit: bool) -> String {
    format!(
        "#!/bin/sh\nlog=\"$0.log\"\nprintf '%s\\n' \"$*\" >> \"$log\"\necho \"stderr:$*\" >&2\ncase \" $* \" in\n  *\" up \"*) if {fail_up}; then exit 9; fi ;;\n  *\"kafka-broker-api-versions.sh\"*)\n    ready=\"$0.ready\"\n    if {persistent_startup_exit} || [ ! -e \"$ready\" ]; then\n      : > \"$ready\"\n      if {startup_exit}; then : > \"$0.exited\"; fi\n      exit 1\n    fi ;;\n  *\" ps \"*\" --status exited \"*)\n    if [ -e \"$0.exited\" ]; then echo broker; fi\n    exit 0 ;;\n  *\" logs \"*) echo \"error while preparing configs: fixture startup exit\" ;;\n  *\" start broker \"*) rm -f \"$0.exited\" ;;\n  *) echo \"stdout:$*\" ;;\nesac\nexit 0\n"
    )
}

#[cfg(unix)]
fn make_executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = fs::metadata(path)
        .unwrap_or_else(|error| panic!("inspect fake Docker program: {error}"))
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)
        .unwrap_or_else(|error| panic!("make fake Docker program executable: {error}"));
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) {}
