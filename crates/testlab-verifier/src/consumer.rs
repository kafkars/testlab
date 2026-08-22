//! Consumer verification compares packaged receive bytes to prior scenario sends.

use testlab_schema::{
    ByteString, ConsumedRecord, OperationId, RecordSpec, Scenario, ScenarioAction, Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify_consumers(
    scenario: &Scenario,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    for step in &scenario.steps {
        let Some((receive_id, expected_operation_id, group)) = expectation(&step.action) else {
            continue;
        };
        if !index.action_issued(&step.action) {
            continue;
        }
        let receives = index.receives.get(receive_id);
        let matching_kind = receives
            .and_then(|values| values.first())
            .is_some_and(|receive| receive.committed.is_some() == group);
        if receives.map_or(0, Vec::len) != 1 || !matching_kind {
            violations.push(violation(
                "CONS-001",
                format!(
                    "receive {receive_id} expected one completion, observed {}",
                    receives.map_or(0, Vec::len)
                ),
                Some(receive_id.clone()),
                receive_references(receives.map(Vec::as_slice)),
            ));
            continue;
        }
        let Some(expected) = sent_record(scenario, expected_operation_id) else {
            continue;
        };
        let receive = &receives.map_or(&[][..], Vec::as_slice)[0];
        if group && receive.committed != Some(true) {
            violations.push(violation(
                "CONS-003",
                format!("group receive {receive_id} did not commit its checkpoint"),
                Some(receive_id.clone()),
                vec![format!("history:{}", receive.history_sequence)],
            ));
        }
        let exact = receive
            .records
            .iter()
            .filter(|record| exact_record(record, expected))
            .count();
        if exact != 1 {
            violations.push(violation(
                "CONS-002",
                format!(
                    "receive {receive_id} expected exactly one public record matching send {expected_operation_id}, observed {exact}"
                ),
                Some(expected_operation_id.clone()),
                vec![format!("history:{}", receive.history_sequence)],
            ));
        }
    }
}

fn expectation(action: &ScenarioAction) -> Option<(&OperationId, &OperationId, bool)> {
    match action {
        ScenarioAction::Receive {
            receive_id,
            expected_operation_id,
            ..
        } => Some((receive_id, expected_operation_id, false)),
        ScenarioAction::GroupReceive {
            receive_id,
            expected_operation_id,
            ..
        } => Some((receive_id, expected_operation_id, true)),
        _ => None,
    }
}

fn sent_record<'a>(scenario: &'a Scenario, target: &OperationId) -> Option<&'a RecordSpec> {
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::Send {
                operation_id,
                record,
                ..
            } if operation_id == target => return Some(record),
            ScenarioAction::SendBatch { operations, .. } => {
                if let Some(operation) = operations
                    .iter()
                    .find(|operation| &operation.operation_id == target)
                {
                    return Some(&operation.record);
                }
            }
            _ => {}
        }
    }
    None
}

fn exact_record(actual: &ConsumedRecord, expected: &RecordSpec) -> bool {
    actual.topic == expected.topic
        && actual.partition == expected.partition
        && exact_bytes(actual.key.as_ref(), expected.key.as_ref())
        && exact_bytes(actual.value.as_ref(), expected.value.as_ref())
        && actual.headers.len() == expected.headers.len()
        && actual
            .headers
            .iter()
            .zip(&expected.headers)
            .all(|(left, right)| {
                left.name == right.name && exact_bytes(left.value.as_ref(), right.value.as_ref())
            })
}

fn exact_bytes(left: Option<&ByteString>, right: Option<&ByteString>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => match (left.decode(), right.decode()) {
            (Ok(left), Ok(right)) => left == right,
            _ => false,
        },
        _ => false,
    }
}

fn receive_references(values: Option<&[crate::index::IndexedReceive]>) -> Vec<String> {
    values
        .into_iter()
        .flatten()
        .map(|receive| format!("history:{}", receive.history_sequence))
        .collect()
}
