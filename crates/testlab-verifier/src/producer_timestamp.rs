//! Explicit producer timestamps join scenario intent to public and broker truth.

use std::collections::BTreeMap;

use testlab_schema::{BrokerObservation, OperationId, RecordSpec, TerminalStatus, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(
    sends: &BTreeMap<OperationId, RecordSpec>,
    index: &HistoryIndex,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    for (operation_id, record) in sends {
        let Some(expected) = record.timestamp_millis else {
            continue;
        };
        let Some([terminal]) = index.terminals.get(operation_id).map(Vec::as_slice) else {
            continue;
        };
        if !matches!(
            terminal.status,
            TerminalStatus::Acknowledged | TerminalStatus::TransactionStaged
        ) {
            continue;
        }
        let Some([observation]) = observed.get(operation_id).map(Vec::as_slice) else {
            continue;
        };
        if terminal.timestamp_millis == Some(expected)
            && observation.record.timestamp_millis == Some(expected)
        {
            continue;
        }
        violations.push(violation(
            "PROD-013",
            format!(
                "operation {operation_id} expected public and broker timestamp {expected}, observed public {:?} and broker {:?}",
                terminal.timestamp_millis, observation.record.timestamp_millis
            ),
            Some(operation_id.clone()),
            vec![
                format!("history:{}", terminal.history_sequence),
                format!("broker-observation:{}", observation.observation),
            ],
        ));
    }
}
