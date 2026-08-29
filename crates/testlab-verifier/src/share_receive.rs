//! Share receive verification binds public batches to scenario intent and broker-backed records.

use testlab_schema::{
    ByteString, ConsumedRecord, ConsumerId, OperationId, RecordSpec, Scenario, ScenarioAction,
    Violation,
};

use crate::index::{HistoryIndex, IndexedShareReceive};
use crate::support::violation;

#[derive(Clone, Copy)]
pub(crate) struct Expectation<'a> {
    pub(crate) consumer_id: &'a ConsumerId,
    pub(crate) receive_id: &'a OperationId,
    pub(crate) operation_ids: &'a [OperationId],
    pub(crate) minimum_delivery_count: i16,
    pub(crate) acquisition_count: Option<usize>,
}

pub(crate) fn verify(
    scenario: &Scenario,
    index: &HistoryIndex,
    expectation: Expectation<'_>,
    violations: &mut Vec<Violation>,
) {
    let receives = index.share_receives.get(expectation.receive_id);
    if receives.map_or(0, Vec::len) != 1 {
        violations.push(violation(
            "SHARE-002",
            format!(
                "share receive {} expected one completion, observed {}",
                expectation.receive_id,
                receives.map_or(0, Vec::len)
            ),
            Some(expectation.receive_id.clone()),
            references(receives.map(Vec::as_slice)),
        ));
        return;
    }
    let receive = &receives.map_or(&[][..], Vec::as_slice)[0];
    verify_acquisitions(receive, expectation, violations);
    verify_fences(receive, expectation, violations);
    verify_records(scenario, receive, expectation, violations);
}

fn verify_acquisitions(
    receive: &IndexedShareReceive,
    expectation: Expectation<'_>,
    violations: &mut Vec<Violation>,
) {
    if expectation
        .acquisition_count
        .is_some_and(|expected| receive.acquisition_count != expected)
    {
        violations.push(violation(
            "SHARE-010",
            format!(
                "share receive {} expected acquisition count {:?}, observed {}",
                expectation.receive_id, expectation.acquisition_count, receive.acquisition_count
            ),
            Some(expectation.receive_id.clone()),
            vec![format!("history:{}", receive.history_sequence)],
        ));
    }
}

fn verify_fences(
    receive: &IndexedShareReceive,
    expectation: Expectation<'_>,
    violations: &mut Vec<Violation>,
) {
    if &receive.consumer_id != expectation.consumer_id
        || receive.member_epoch.is_none_or(|epoch| epoch <= 0)
        || receive.assignment_epoch.is_none_or(|epoch| epoch == 0)
    {
        violations.push(violation(
            "SHARE-005",
            format!(
                "share receive {} did not retain positive membership fences",
                expectation.receive_id
            ),
            Some(expectation.receive_id.clone()),
            vec![format!("history:{}", receive.history_sequence)],
        ));
    }
}

fn verify_records(
    scenario: &Scenario,
    receive: &IndexedShareReceive,
    expectation: Expectation<'_>,
    violations: &mut Vec<Violation>,
) {
    let Some(expected) = expectation
        .operation_ids
        .iter()
        .map(|operation_id| sent_record(scenario, operation_id))
        .collect::<Option<Vec<_>>>()
    else {
        return;
    };
    let matches = receive.records.len() == expected.len()
        && receive
            .records
            .iter()
            .zip(&expected)
            .all(|(record, expected)| {
                exact_record(&record.record, expected)
                    && record.delivery_count >= expectation.minimum_delivery_count
            });
    if matches {
        return;
    }
    let contract = if expectation.minimum_delivery_count > 1 {
        "SHARE-009"
    } else if expectation.operation_ids.len() > 1 {
        "SHARE-007"
    } else {
        "SHARE-002"
    };
    violations.push(violation(
        contract,
        format!(
            "share receive {} expected exact ordered records from {:?} with delivery count at least {}",
            expectation.receive_id,
            expectation.operation_ids,
            expectation.minimum_delivery_count
        ),
        Some(expectation.receive_id.clone()),
        vec![format!("history:{}", receive.history_sequence)],
    ));
}

fn sent_record<'a>(scenario: &'a Scenario, target: &OperationId) -> Option<&'a RecordSpec> {
    scenario.steps.iter().find_map(|step| match &step.action {
        ScenarioAction::Send {
            operation_id,
            record,
            ..
        } if operation_id == target => Some(record),
        ScenarioAction::SendBatch { operations, .. } => operations
            .iter()
            .find(|operation| &operation.operation_id == target)
            .map(|operation| &operation.record),
        _ => None,
    })
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
        (Some(left), Some(right)) => matches!(
            (left.decode(), right.decode()),
            (Ok(left), Ok(right)) if left == right
        ),
        _ => false,
    }
}

fn references(values: Option<&[IndexedShareReceive]>) -> Vec<String> {
    values
        .into_iter()
        .flatten()
        .map(|receive| format!("history:{}", receive.history_sequence))
        .collect()
}
