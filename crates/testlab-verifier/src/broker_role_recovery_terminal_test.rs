//! Recovery terminals retain exact Compose identity and every readiness attempt.

use testlab_schema::{
    EnvironmentOperation, EnvironmentOperationStatus, HistoryEntry, HistoryPayload,
};

use crate::broker_role_recovery::verify;
use crate::broker_role_recovery_test::{history, scenario, target};
use crate::index::HistoryIndex;

#[test]
fn restart_with_failed_probes_then_exact_readiness_passes() {
    assert!(violations(&restart_history()).is_empty());
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
