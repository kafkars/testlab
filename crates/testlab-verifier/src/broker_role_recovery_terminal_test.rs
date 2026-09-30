//! Recovery terminals retain exact Compose identity and every readiness attempt.

use testlab_schema::{
    BrokerRoleTarget, EnvironmentOperation, EnvironmentOperationKind, EnvironmentOperationStatus,
    HistoryEntry, HistoryPayload,
};

use crate::broker_role_recovery::verify;
use crate::broker_role_recovery_test::{history, scenario, target};
use crate::index::HistoryIndex;

#[test]
fn restart_with_failed_probes_then_exact_readiness_passes() {
    assert!(violations(&restart_history()).is_empty());
}

#[test]
fn separate_role_outages_on_the_same_broker_keep_separate_restore_windows() {
    let entries = repeated_broker_history();
    for role in [
        target(),
        BrokerRoleTarget::PartitionLeader {
            topic: "other-records".to_owned(),
            partition: 0,
        },
    ] {
        let mut failures = Vec::new();
        verify(
            &scenario(role),
            &HistoryIndex::build(&entries),
            &mut failures,
        );
        assert!(failures.is_empty(), "{failures:?}");
    }
}

#[test]
fn later_outage_cannot_supply_a_missing_restore_or_hide_a_duplicate() {
    let mut missing = repeated_broker_history();
    missing.retain(|entry| !(4..=6).contains(&entry.sequence));
    assert!(
        violations(&missing)
            .iter()
            .any(|value| value == "FAULT-002")
    );

    let mut duplicate = repeated_broker_history();
    let mut extra_start = duplicate[4].clone();
    extra_start.sequence = 8;
    duplicate.insert(7, extra_start);
    assert!(
        violations(&duplicate)
            .iter()
            .any(|value| value == "FAULT-002")
    );
}

#[test]
fn missing_interrupted_foreign_or_unsuccessful_readiness_fails() {
    for mutation in 0..9 {
        let mut entries = restart_history();
        match mutation {
            0 => {
                entries.pop();
            }
            1 => entries[6].sequence += 1,
            2 => operation(&mut entries[6]).args[3] = "broker-9".to_owned(),
            3 => operation(&mut entries[6]).status = EnvironmentOperationStatus::Failed,
            4 => operation(&mut entries[4]).args[1] = "start".to_owned(),
            5 => operation(&mut entries[4])
                .args
                .insert(1, "--different-project".to_owned()),
            6 => operation(&mut entries[6]).program = "echo".to_owned(),
            7 => operation(&mut entries[5]).status = EnvironmentOperationStatus::TimedOut,
            _ => operation(&mut entries[6])
                .args
                .insert(1, "--different-project".to_owned()),
        }
        assert!(
            violations(&entries)
                .iter()
                .any(|value| value == "FAULT-002"),
            "mutation {mutation}"
        );
    }
}

fn restart_history() -> Vec<HistoryEntry> {
    let mut entries = history(2, true);
    operation(&mut entries[4]).args = ["compose", "restart", "--no-deps", "broker-1"]
        .map(str::to_owned)
        .to_vec();
    let mut ready = entries[5].clone();
    ready.sequence = 6;
    ready.observed_unix_ms = 6;
    operation(&mut entries[5]).status = EnvironmentOperationStatus::Failed;
    entries.push(ready);
    entries
}

fn repeated_broker_history() -> Vec<HistoryEntry> {
    let mut entries = restart_history();
    let later = restart_history().into_iter().map(|mut entry| {
        entry.sequence += 10;
        entry.observed_unix_ms += 10;
        if let HistoryPayload::EnvironmentOperation { operation } = &mut entry.payload
            && operation.kind == EnvironmentOperationKind::BrokerRoleObserve
        {
            operation.args[1] = "other-records".to_owned();
        }
        entry
    });
    entries.extend(later);
    entries
}

fn operation(entry: &mut HistoryEntry) -> &mut EnvironmentOperation {
    let HistoryPayload::EnvironmentOperation { operation } = &mut entry.payload else {
        panic!("environment operation fixture");
    };
    operation
}

fn violations(entries: &[HistoryEntry]) -> Vec<String> {
    let mut violations = Vec::new();
    verify(
        &scenario(target()),
        &HistoryIndex::build(entries),
        &mut violations,
    );
    violations
        .into_iter()
        .map(|value| value.contract_id.to_string())
        .collect()
}
