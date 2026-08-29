//! Recorder tests prove one stable order across commands and external controls.

use testlab_schema::{
    AdapterCommand, BrokerBehavior, BrokerConsumerGroupOffset, BrokerStateObservation,
    CommandEnvelope, CommandId, EnvironmentOperation, EnvironmentOperationId,
    EnvironmentOperationKind, EnvironmentOperationStatus, HistoryPayload,
    NetworkConnectionCutAction, NetworkConnectionsCutObservation, NetworkProxyControl,
    NetworkProxyObservation, OperationId,
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
    if let Err(error) = recorder.state_observation(state_observation()) {
        panic!("failed to record broker state: {error}");
    }
    if let Err(error) = recorder.network_proxy_control(network_control()) {
        panic!("failed to record network control: {error}");
    }
    if let Err(error) = recorder.network_proxy_observation(network_observation()) {
        panic!("failed to record network observation: {error}");
    }
    assert_eq!(recorder.entries()[0].sequence, 0);
    assert_eq!(recorder.entries()[1].sequence, 1);
    assert_eq!(recorder.entries()[2].sequence, 2);
    assert_eq!(recorder.entries()[3].sequence, 3);
    assert_eq!(recorder.entries()[4].sequence, 4);
    assert_eq!(recorder.entries()[5].sequence, 5);
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
    assert!(matches!(
        &recorder.entries()[3].payload,
        HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::ConsumerGroupOffset(BrokerConsumerGroupOffset {
                offset: Some(1),
                ..
            })
        }
    ));
    assert!(matches!(
        &recorder.entries()[4].payload,
        HistoryPayload::NetworkProxyControl {
            control: NetworkProxyControl::CutConnections(action)
        } if action.operation_id.as_str() == "cut-1"
    ));
    assert!(matches!(
        &recorder.entries()[5].payload,
        HistoryPayload::NetworkProxyObservation {
            observation: NetworkProxyObservation::ConnectionsCut(value)
        } if value.connections_cut == 2
    ));
}

fn network_control() -> NetworkProxyControl {
    NetworkProxyControl::CutConnections(NetworkConnectionCutAction {
        operation_id: EnvironmentOperationId::new("cut-1")
            .unwrap_or_else(|error| panic!("invalid cut id: {error}")),
        broker_ordinal: 1,
        timeout_ms: 1_000,
    })
}

fn network_observation() -> NetworkProxyObservation {
    NetworkProxyObservation::ConnectionsCut(NetworkConnectionsCutObservation {
        observation: 0,
        operation_id: EnvironmentOperationId::new("cut-1")
            .unwrap_or_else(|error| panic!("invalid cut id: {error}")),
        broker_ordinal: 1,
        connections_cut: 2,
        completed_unix_ms: 1,
    })
}

fn state_observation() -> BrokerStateObservation {
    BrokerStateObservation::ConsumerGroupOffset(BrokerConsumerGroupOffset {
        observation: 0,
        operation_id: OperationId::new("admin-offset-1")
            .unwrap_or_else(|error| panic!("invalid operation id: {error}")),
        group_id: "group-1".to_owned(),
        topic: "records".to_owned(),
        partition: 0,
        offset: Some(1),
    })
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
