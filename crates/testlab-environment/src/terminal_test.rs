//! Terminal supervision tests prove bounded completion evidence.

use std::path::PathBuf;
use std::time::Duration;

use testlab_schema::{
    EnvironmentOperationId, EnvironmentOperationKind, EnvironmentOperationStatus,
};

use super::{TerminalRequest, run_terminal};

#[test]
fn successful_command_captures_both_streams() {
    let output = run_terminal(request(
        "printf stdout; printf stderr >&2",
        Duration::from_secs(2),
    ));

    assert!(output.succeeded());
    assert_eq!(output.stdout, b"stdout");
    assert_eq!(output.stderr, b"stderr");
    assert_eq!(output.operation.exit_code, Some(0));
}

#[test]
fn failed_command_retains_exit_status() {
    let output = run_terminal(request("exit 7", Duration::from_secs(2)));

    assert_eq!(output.operation.status, EnvironmentOperationStatus::Failed);
    assert_eq!(output.operation.exit_code, Some(7));
}

#[test]
fn timed_out_command_is_killed_and_waited() {
    let output = run_terminal(request("sleep 2", Duration::from_millis(20)));

    assert_eq!(
        output.operation.status,
        EnvironmentOperationStatus::TimedOut
    );
}

fn request(script: &str, timeout: Duration) -> TerminalRequest {
    TerminalRequest {
        id: EnvironmentOperationId::new("environment-00000001")
            .unwrap_or_else(|error| panic!("fixture operation id: {error}")),
        kind: EnvironmentOperationKind::ComposeConfig,
        program: "sh".to_owned(),
        args: vec!["-c".to_owned(), script.to_owned()],
        current_directory: PathBuf::from("."),
        environment: Vec::new(),
        started_unix_ms: 1,
        timeout,
        stdout_artifact: Some("stdout.txt".to_owned()),
        stderr_artifact: Some("stderr.txt".to_owned()),
    }
}
