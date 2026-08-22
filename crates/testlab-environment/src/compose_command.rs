//! Compose command construction keeps effect descriptions portable and reviewable.

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

fn compose_owned(
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
