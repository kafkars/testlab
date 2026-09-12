//! Explicit producer timestamp tests distinguish public and broker mismatches.

use testlab_schema::{
    AdapterEvent, HistoryPayload, ScenarioAction, TerminalStatus, VisibilityExpectation,
};

use crate::verify;
use crate::verify_fixture::{adapter, history, observation, scenario};

const TIMESTAMP_MILLIS: i64 = 1_700_000_000_123;

#[test]
fn exact_public_and_broker_timestamps_pass() {
    let scenario = timestamp_scenario();
    let mut history = history(TerminalStatus::Acknowledged);
    set_terminal_timestamp(&mut history, Some(TIMESTAMP_MILLIS));
    let mut observed = observation(0, "value");
    observed.record.timestamp_millis = Some(TIMESTAMP_MILLIS);
    observed.digest = observed.record.digest().unwrap_or_default();

    let verdict = verify(&scenario, &adapter(), &history, &[observed]);

    assert!(verdict.violations.is_empty());
}

#[test]
fn public_timestamp_mismatch_fails() {
    let scenario = timestamp_scenario();
    let mut history = history(TerminalStatus::Acknowledged);
    set_terminal_timestamp(&mut history, Some(TIMESTAMP_MILLIS + 1));
    let mut observed = observation(0, "value");
    observed.record.timestamp_millis = Some(TIMESTAMP_MILLIS);
    observed.digest = observed.record.digest().unwrap_or_default();

    let verdict = verify(&scenario, &adapter(), &history, &[observed]);

    assert!(
        verdict
            .violations
            .iter()
            .any(|value| value.contract_id.as_str() == "PROD-013")
    );
}

#[test]
fn broker_timestamp_mismatch_fails() {
    let scenario = timestamp_scenario();
    let mut history = history(TerminalStatus::Acknowledged);
    set_terminal_timestamp(&mut history, Some(TIMESTAMP_MILLIS));
    let mut observed = observation(0, "value");
    observed.record.timestamp_millis = Some(TIMESTAMP_MILLIS + 1);
    observed.digest = observed.record.digest().unwrap_or_default();

    let verdict = verify(&scenario, &adapter(), &history, &[observed]);

    assert!(
        verdict
            .violations
            .iter()
            .any(|value| value.contract_id.as_str() == "PROD-013")
    );
}

fn timestamp_scenario() -> testlab_schema::Scenario {
    let mut value = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    let record = value
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::Send { record, .. } => Some(record),
            _ => None,
        });
    record
        .unwrap_or_else(|| panic!("fixture scenario omitted its send"))
        .timestamp_millis = Some(TIMESTAMP_MILLIS);
    value
}

fn set_terminal_timestamp(
    history: &mut [testlab_schema::HistoryEntry],
    timestamp_millis: Option<i64>,
) {
    for entry in history {
        if let HistoryPayload::AdapterEvent { event } = &mut entry.payload
            && let AdapterEvent::OperationTerminal {
                timestamp_millis: actual,
                ..
            } = &mut event.event
        {
            *actual = timestamp_millis;
        }
    }
}
