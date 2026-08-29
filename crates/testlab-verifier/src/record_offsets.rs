//! Offset verification binds public coordinates and caller order to broker observations.

use std::collections::BTreeMap;

use testlab_schema::{
    BrokerObservation, OperationId, Scenario, ScenarioAction, TerminalStatus, Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;
use crate::verify_index::observations_by_operation;

pub(crate) fn verify(
    scenario: &Scenario,
    index: &HistoryIndex,
    observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) {
    let observed = observations_by_operation(observations);
    verify_terminal_offsets(index, &observed, violations);
    crate::record_consumers::verify(scenario, index, &observed, violations);
    verify_partition_order(scenario, index, &observed, violations);
}

fn verify_terminal_offsets(
    index: &HistoryIndex,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    for (operation_id, terminals) in &index.terminals {
        let [terminal] = terminals.as_slice() else {
            continue;
        };
        let expected = match terminal.status {
            TerminalStatus::Acknowledged | TerminalStatus::TransactionStaged => {
                let Some([observation]) = observed.get(operation_id).map(Vec::as_slice) else {
                    continue;
                };
                Some(observation.offset)
            }
            TerminalStatus::DefinitelyNotSent | TerminalStatus::PossiblySent => None,
        };
        if terminal.offset != expected {
            let mut evidence = vec![format!("history:{}", terminal.history_sequence)];
            evidence.extend(observation_references(
                observed.get(operation_id).map(Vec::as_slice),
            ));
            violations.push(violation(
                "PROD-010",
                format!(
                    "public terminal offset {:?} did not match independently observed offset {expected:?}",
                    terminal.offset
                ),
                Some(operation_id.clone()),
                evidence,
            ));
        }
    }
}

fn verify_partition_order(
    scenario: &Scenario,
    index: &HistoryIndex,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    let mut partitions: BTreeMap<(String, i32), Vec<OperationId>> = BTreeMap::new();
    for step in &scenario.steps {
        if !index.action_issued(&step.action) {
            continue;
        }
        match &step.action {
            ScenarioAction::Send {
                operation_id,
                record,
                ..
            } => partitions
                .entry((record.topic.clone(), record.partition))
                .or_default()
                .push(operation_id.clone()),
            ScenarioAction::SendBatch { operations, .. } => {
                for operation in operations {
                    partitions
                        .entry((operation.record.topic.clone(), operation.record.partition))
                        .or_default()
                        .push(operation.operation_id.clone());
                }
            }
            _ => {}
        }
    }
    for operations in partitions.values() {
        for pair in operations.windows(2) {
            verify_ordered_pair(&pair[0], &pair[1], observed, violations);
        }
    }
}

fn verify_ordered_pair(
    before: &OperationId,
    after: &OperationId,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    let Some([before_observation]) = observed.get(before).map(Vec::as_slice) else {
        return;
    };
    let Some([after_observation]) = observed.get(after).map(Vec::as_slice) else {
        return;
    };
    if before_observation.offset >= after_observation.offset {
        violations.push(violation(
            "PROD-011",
            format!(
                "declared operation {before} at offset {} did not precede {after} at offset {}",
                before_observation.offset, after_observation.offset
            ),
            Some(after.clone()),
            vec![
                format!("broker-observation:{}", before_observation.observation),
                format!("broker-observation:{}", after_observation.observation),
            ],
        ));
    }
}

fn observation_references(observations: Option<&[&BrokerObservation]>) -> Vec<String> {
    observations
        .into_iter()
        .flatten()
        .map(|observation| format!("broker-observation:{}", observation.observation))
        .collect()
}
