//! Recorder tests prove one stable order across commands and external controls.

use testlab_schema::{
    AdapterCommand, BrokerBehavior, CommandEnvelope, CommandId, EnvironmentOperation,
    EnvironmentOperationId, EnvironmentOperationKind, EnvironmentOperationStatus, HistoryPayload,
};

use crate::recorder::HistoryRecorder;

#[test]
fn recorder_assigns_monotonic_sequences() {
    let command_id = match CommandId::new("command-1") {
        Ok(command_id) => command_id,
        Err(error) => panic!("invalid fixture command id: {error}"),
    };
    let mut recorder = HistoryRecorder::default();
    if let Err(error) = recorder.command(CommandEnvelope::new(command_id, AdapterCommand::Finish)) {
        panic!("failed to record command: {error}");
    }
    if let Err(error) = recorder.broker_control(BrokerBehavior::Acknowledge) {
        panic!("failed to record broker control: {error}");
    }
    if let Err(error) = recorder.environment_operation(operation()) {
        panic!("failed to record environment operation: {error}");
    }
    assert_eq!(recorder.entries()[0].sequence, 0);
    assert_eq!(recorder.entries()[1].sequence, 1);
    assert_eq!(recorder.entries()[2].sequence, 2);
    assert!(matches!(
        &recorder.entries()[1].payload,
        HistoryPayload::BrokerControl {
            behavior: BrokerBehavior::Acknowledge
        }
    ));
    assert!(matches!(
        &recorder.entries()[2].payload,
        HistoryPayload::EnvironmentOperation { operation }
            if operation.kind == EnvironmentOperationKind::ComposeConfig
    ));
}

fn operation() -> EnvironmentOperation {
    EnvironmentOperation {
        id: EnvironmentOperationId::new("environment-00000001")
            .unwrap_or_else(|error| panic!("invalid operation id: {error}")),
        kind: EnvironmentOperationKind::ComposeConfig,
        program: "docker".to_owned(),
        args: vec!["compose".to_owned(), "config".to_owned()],
        started_unix_ms: 1,
        completed_unix_ms: 2,
        status: EnvironmentOperationStatus::Succeeded,
        exit_code: Some(0),
        stdout_artifact: Some("compose-config.yml".to_owned()),
        stderr_artifact: None,
        diagnostic: None,
    }
}
