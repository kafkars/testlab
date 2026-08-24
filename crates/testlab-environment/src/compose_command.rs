//! Compose command construction keeps effect descriptions portable and reviewable.

use std::path::Path;

use testlab_schema::EnvironmentOperationKind;

#[derive(Clone, Debug)]
pub(super) struct CommandSpec {
    pub(super) kind: EnvironmentOperationKind,
    pub(super) args: Vec<String>,
    pub(super) stdout_artifact: String,
    pub(super) stderr_artifact: String,
}

pub(super) fn image_pull(image: &str) -> CommandSpec {
    CommandSpec {
        kind: EnvironmentOperationKind::ImagePull,
        args: vec!["pull".to_owned(), image.to_owned()],
        stdout_artifact: "image-pull.txt".to_owned(),
        stderr_artifact: "image-pull.stderr.txt".to_owned(),
    }
}

pub(super) fn image_inspect(image: &str) -> CommandSpec {
    CommandSpec {
        kind: EnvironmentOperationKind::ImageInspect,
        args: vec!["image".to_owned(), "inspect".to_owned(), image.to_owned()],
        stdout_artifact: "image-inspect.json".to_owned(),
        stderr_artifact: "image-inspect.stderr.txt".to_owned(),
    }
}

pub(super) fn config(prefix: &[String]) -> CommandSpec {
    compose(
        EnvironmentOperationKind::ComposeConfig,
        prefix,
        &["config"],
        "compose-config.yml",
        "compose-config.stderr.txt",
    )
}

pub(super) fn up(prefix: &[String]) -> CommandSpec {
    compose(
        EnvironmentOperationKind::ComposeUp,
        prefix,
        &["up", "--detach", "--no-build", "--remove-orphans"],
        "compose-up.txt",
        "compose-up.stderr.txt",
    )
}

pub(super) fn restart(prefix: &[String], service: &str, operation: u32) -> CommandSpec {
    compose_owned(
        EnvironmentOperationKind::BrokerRestart,
        prefix,
        vec![
            "restart".to_owned(),
            "--no-deps".to_owned(),
            service.to_owned(),
        ],
        format!("broker-restart-{service}-{operation:05}.txt"),
        format!("broker-restart-{service}-{operation:05}.stderr.txt"),
    )
}

pub(super) fn stop(prefix: &[String], service: &str, operation: u32) -> CommandSpec {
    compose_owned(
        EnvironmentOperationKind::BrokerStop,
        prefix,
        vec!["stop".to_owned(), service.to_owned()],
        format!("broker-stop-{service}-{operation:05}.txt"),
        format!("broker-stop-{service}-{operation:05}.stderr.txt"),
    )
}

pub(super) fn start(prefix: &[String], service: &str, operation: u32) -> CommandSpec {
    compose_owned(
        EnvironmentOperationKind::BrokerStart,
        prefix,
        vec!["start".to_owned(), service.to_owned()],
        format!("broker-start-{service}-{operation:05}.txt"),
        format!("broker-start-{service}-{operation:05}.stderr.txt"),
    )
}

pub(super) fn restart_readiness(
    prefix: &[String],
    service: &str,
    client_port: u16,
    operation: u32,
    attempt: u32,
) -> CommandSpec {
    let port = format!("localhost:{client_port}");
    compose_owned(
        EnvironmentOperationKind::Readiness,
        prefix,
        vec![
            "exec".to_owned(),
            "--no-TTY".to_owned(),
            service.to_owned(),
            "/opt/kafka/bin/kafka-broker-api-versions.sh".to_owned(),
            "--bootstrap-server".to_owned(),
            port,
        ],
        format!("broker-restart-readiness-{service}-{operation:05}-{attempt:03}.txt"),
        format!("broker-restart-readiness-{service}-{operation:05}-{attempt:03}.stderr.txt"),
    )
}

pub(super) fn readiness(
    prefix: &[String],
    service: &str,
    client_port: u16,
    attempt: u32,
) -> CommandSpec {
    let stdout = format!("readiness-{service}-{attempt:03}.txt");
    let stderr = format!("readiness-{service}-{attempt:03}.stderr.txt");
    let port = format!("localhost:{client_port}");
    compose_owned(
        EnvironmentOperationKind::Readiness,
        prefix,
        vec![
            "exec".to_owned(),
            "--no-TTY".to_owned(),
            service.to_owned(),
            "/opt/kafka/bin/kafka-broker-api-versions.sh".to_owned(),
            "--bootstrap-server".to_owned(),
            port,
        ],
        stdout,
        stderr,
    )
}

pub(super) fn scram_setup(
    prefix: &[String],
    service: &str,
    client_port: u16,
    mechanism: &str,
) -> CommandSpec {
    let command = format!(
        "/opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:{client_port} \
         --alter --entity-type users --entity-name kafkars \
         --add-config \"{mechanism}=[iterations=8192,password=$TESTLAB_SCRAM_PASSWORD]\""
    );
    compose_owned(
        EnvironmentOperationKind::BrokerSecuritySetup,
        prefix,
        vec![
            "exec".to_owned(),
            "--no-TTY".to_owned(),
            "--env".to_owned(),
            "TESTLAB_SCRAM_PASSWORD".to_owned(),
            service.to_owned(),
            "/bin/bash".to_owned(),
            "-euc".to_owned(),
            command,
        ],
        "security-setup.txt".to_owned(),
        "security-setup.stderr.txt".to_owned(),
    )
}

pub(super) fn feature_setup(
    prefix: &[String],
    service: &str,
    client_port: u16,
    name: &str,
    level: u16,
) -> CommandSpec {
    compose_owned(
        EnvironmentOperationKind::BrokerFeatureSetup,
        prefix,
        vec![
            "exec".to_owned(),
            "--no-TTY".to_owned(),
            service.to_owned(),
            "/opt/kafka/bin/kafka-features.sh".to_owned(),
            "--bootstrap-server".to_owned(),
            format!("localhost:{client_port}"),
            "upgrade".to_owned(),
            "--feature".to_owned(),
            format!("{name}={level}"),
        ],
        format!("broker-feature-{name}-{level}.txt"),
        format!("broker-feature-{name}-{level}.stderr.txt"),
    )
}

pub(super) fn copy_tls_ca(prefix: &[String], service: &str, destination: &Path) -> CommandSpec {
    compose_owned(
        EnvironmentOperationKind::BrokerSecuritySetup,
        prefix,
        vec![
            "cp".to_owned(),
            format!("{service}:/etc/kafka/secrets/ca.pem"),
            destination.display().to_string(),
        ],
        "security-ca-copy.txt".to_owned(),
        "security-ca-copy.stderr.txt".to_owned(),
    )
}

pub(super) fn ps(prefix: &[String]) -> CommandSpec {
    compose(
        EnvironmentOperationKind::ComposePs,
        prefix,
        &["ps", "--all", "--format", "json"],
        "compose-ps.json",
        "compose-ps.stderr.txt",
    )
}

pub(super) fn logs(prefix: &[String], services: &[String]) -> CommandSpec {
    let mut tail = vec![
        "logs".to_owned(),
        "--no-color".to_owned(),
        "--timestamps".to_owned(),
    ];
    tail.extend_from_slice(services);
    compose_owned(
        EnvironmentOperationKind::ComposeLogs,
        prefix,
        tail,
        "broker.log".to_owned(),
        "broker-log.stderr.txt".to_owned(),
    )
}

pub(super) fn down(prefix: &[String]) -> CommandSpec {
    compose(
        EnvironmentOperationKind::ComposeDown,
        prefix,
        &["down", "--volumes", "--remove-orphans"],
        "compose-down.txt",
        "compose-down.stderr.txt",
    )
}

fn compose(
    kind: EnvironmentOperationKind,
    prefix: &[String],
    tail: &[&str],
    stdout: &str,
    stderr: &str,
) -> CommandSpec {
    compose_owned(
        kind,
        prefix,
        tail.iter().map(|value| (*value).to_owned()).collect(),
        stdout.to_owned(),
        stderr.to_owned(),
    )
}

pub(super) fn compose_owned(
    kind: EnvironmentOperationKind,
    prefix: &[String],
    tail: Vec<String>,
    stdout_artifact: String,
    stderr_artifact: String,
) -> CommandSpec {
    let mut args = prefix.to_vec();
    args.extend(tail);
    CommandSpec {
        kind,
        args,
        stdout_artifact,
        stderr_artifact,
    }
}
