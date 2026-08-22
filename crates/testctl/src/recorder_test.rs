//! Recorder tests prove one stable order across commands and external controls.

use testlab_schema::{AdapterCommand, BrokerBehavior, CommandEnvelope, CommandId, HistoryPayload};

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
    assert_eq!(recorder.entries()[0].sequence, 0);
    assert_eq!(recorder.entries()[1].sequence, 1);
    assert!(matches!(
        &recorder.entries()[1].payload,
        HistoryPayload::BrokerControl {
            behavior: BrokerBehavior::Acknowledge
        }
    ));
}
