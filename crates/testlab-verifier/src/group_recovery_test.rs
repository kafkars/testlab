//! Recovery verifier tests bind every broker disruption to independent terminals and progress.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterEvent, ByteString, ConsumedRecord, EnvironmentOperation, EnvironmentOperationId,
    EnvironmentOperationKind, EnvironmentOperationStatus, GroupMembershipEpoch, HistoryEntry,
    HistoryPayload, OperationId, Scenario, ScenarioAction, ScenarioId,
};

use crate::group_recovery::verify;
use crate::index::HistoryIndex;
use crate::verify_fixture::{event, step};

#[test]
fn three_distinct_disruptions_with_progress_pass() {
    let scenario = recovery_scenario();
    let index = HistoryIndex::build(&recovery_history(true));
    let mut violations = Vec::new();

    verify(&scenario, &index, &mut violations);

    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn disruption_without_progress_fails() {
    let scenario = recovery_scenario();
    let index = HistoryIndex::build(&recovery_history(false));
    let mut violations = Vec::new();

    verify(&scenario, &index, &mut violations);

    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "CONS-011")
    );
}

#[test]
fn overlapping_stop_start_pairs_fail() {
    let scenario = recovery_scenario();
    let base = recovery_history(true);
    let history = [0, 1, 3, 4, 2, 5, 6, 7, 8]
        .into_iter()
        .enumerate()
        .map(|(sequence, index)| {
            let mut entry = base[index].clone();
            entry.sequence = sequence as u64;
            entry.observed_unix_ms = sequence as u64;
            entry
        })
        .collect::<Vec<_>>();
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    verify(&scenario, &index, &mut violations);

    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "CONS-011")
    );
}

fn recovery_scenario() -> Scenario {
    let mut steps = Vec::new();
    for ordinal in 1..=3 {
        steps.push(step(
            &format!("stop-{ordinal}"),
            ScenarioAction::StopBroker {
                broker_ordinal: ordinal,
                timeout_ms: 1_000,
            },
        ));
        steps.push(step(
            &format!("start-{ordinal}"),
            ScenarioAction::StartBroker {
                broker_ordinal: ordinal,
                timeout_ms: 1_000,
            },
        ));
    }
    Scenario {
        schema_version: testlab_schema::SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("consumer.recovery")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "recovery".to_owned(),
        description: "recovery fixture".to_owned(),
        timeout_ms: 10_000,
        requires: BTreeSet::new(),
        steps,
        assertions: Vec::new(),
    }
}

fn recovery_history(include_last_progress: bool) -> Vec<HistoryEntry> {
    let mut history = Vec::new();
    let mut sequence = 0;
    for ordinal in 1..=3 {
        history.push(environment(sequence, ordinal, true));
        sequence += 1;
        if include_last_progress || ordinal < 3 {
            history.push(event(
                sequence,
                AdapterEvent::GroupReceiveCompleted {
                    receive_id: OperationId::new(format!("receive-{ordinal}"))
                        .unwrap_or_else(|error| panic!("receive id: {error}")),
                    records: vec![ConsumedRecord {
                        topic: "records".to_owned(),
                        partition: 0,
                        offset: i64::from(ordinal),
                        timestamp_millis: None,
                        key: None,
                        value: Some(ByteString::utf8(format!("record-{ordinal}"))),
                        headers: Vec::new(),
                    }],
                    committed: true,
                    group_epoch: Some(GroupMembershipEpoch::Classic { generation_id: 1 }),
                },
            ));
            sequence += 1;
        }
        history.push(environment(sequence, ordinal, false));
        sequence += 1;
    }
    history
}

fn environment(sequence: u64, ordinal: u16, stop: bool) -> HistoryEntry {
    let action = if stop { "stop" } else { "start" };
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::EnvironmentOperation {
            operation: EnvironmentOperation {
                id: EnvironmentOperationId::new(format!("{action}-{ordinal}"))
                    .unwrap_or_else(|error| panic!("environment operation id: {error}")),
                kind: if stop {
                    EnvironmentOperationKind::BrokerStop
                } else {
                    EnvironmentOperationKind::BrokerStart
                },
                program: "docker".to_owned(),
                args: vec![format!("broker-{ordinal}")],
                started_unix_ms: sequence,
                completed_unix_ms: sequence,
                status: EnvironmentOperationStatus::Succeeded,
                exit_code: Some(0),
                stdout_artifact: None,
                stderr_artifact: None,
                diagnostic: None,
            },
        },
    }
}
