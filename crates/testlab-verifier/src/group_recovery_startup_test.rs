//! Startup recovery cannot substitute for the three scenario-owned outage windows.

use testlab_schema::{
    EnvironmentOperationId, EnvironmentOperationStatus, HistoryEntry, HistoryPayload,
};

use crate::group_recovery::verify;
use crate::group_recovery_test::{recovery_history, recovery_scenario};
use crate::index::HistoryIndex;

#[test]
fn startup_restart_before_the_first_stop_is_not_a_scenario_restore() {
    let mut violations = Vec::new();
    verify(
        &recovery_scenario(),
        &HistoryIndex::build(&startup_history(true)),
        &mut violations,
    );
    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn startup_restart_cannot_replace_a_missing_scenario_restore() {
    let mut history = startup_history(true);
    let _ = history.remove(6);
    assert_rejected(history);
}

#[test]
fn startup_restart_cannot_hide_a_failed_scenario_restore() {
    let mut history = startup_history(true);
    let HistoryPayload::EnvironmentOperation { operation } = &mut history[6].payload else {
        panic!("scenario restore");
    };
    operation.status = EnvironmentOperationStatus::Failed;
    operation.exit_code = Some(1);
    assert_rejected(history);
}

#[test]
fn duplicate_restore_after_disruption_still_fails() {
    let mut history = startup_history(true);
    let mut duplicate = history[6].clone();
    let HistoryPayload::EnvironmentOperation { operation } = &mut duplicate.payload else {
        panic!("scenario restore");
    };
    operation.id = operation_id("duplicate-restore");
    history.insert(7, duplicate);
    assert_rejected(history);
}

#[test]
fn startup_restart_cannot_supply_missing_group_progress() {
    assert_rejected(startup_history(false));
}

#[test]
fn overlapping_scenario_outages_still_fail_after_startup_recovery() {
    let base = startup_history(true);
    let history = [0, 1, 2, 4, 5, 3, 6, 7, 8, 9]
        .into_iter()
        .map(|position| base[position].clone())
        .collect();
    assert_rejected(history);
}

fn startup_history(include_last_progress: bool) -> Vec<HistoryEntry> {
    let mut history = recovery_history(include_last_progress);
    let mut startup = history[5].clone();
    let HistoryPayload::EnvironmentOperation { operation } = &mut startup.payload else {
        panic!("broker two restore");
    };
    operation.id = operation_id("startup-recovery-broker-2");
    history.insert(0, startup);
    resequence(&mut history);
    history
}

fn assert_rejected(mut history: Vec<HistoryEntry>) {
    resequence(&mut history);
    let mut violations = Vec::new();
    verify(
        &recovery_scenario(),
        &HistoryIndex::build(&history),
        &mut violations,
    );
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "CONS-011"),
        "{violations:?}"
    );
}

fn resequence(history: &mut [HistoryEntry]) {
    for (sequence, entry) in history.iter_mut().enumerate() {
        entry.sequence = sequence as u64;
        entry.observed_unix_ms = entry.sequence;
        if let HistoryPayload::EnvironmentOperation { operation } = &mut entry.payload {
            operation.started_unix_ms = entry.sequence;
            operation.completed_unix_ms = entry.sequence;
        }
    }
}

fn operation_id(value: &str) -> EnvironmentOperationId {
    EnvironmentOperationId::new(value)
        .unwrap_or_else(|error| panic!("environment operation id: {error}"))
}
