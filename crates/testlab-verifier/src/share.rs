//! Share verification binds retained batches, delivery counts, dispositions, and close certainty.

use testlab_schema::{
    ByteString, ConsumedRecord, OperationId, RecordSpec, Scenario, ScenarioAction, TerminalStatus,
    Violation,
};

use crate::index::{HistoryIndex, IndexedShareReceive};
use crate::support::{references, violation};

pub(crate) fn verify_share(
    scenario: &Scenario,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    for step in &scenario.steps {
        if !index.action_issued(&step.action) {
            continue;
        }
        match &step.action {
            ScenarioAction::CreateShareConsumer { consumer_id, .. } => check_count(
                "SHARE-001",
                "share consumer creation",
                index
                    .share_consumers_created
                    .get(consumer_id)
                    .map(Vec::as_slice),
                violations,
            ),
            ScenarioAction::ShareReceive {
                consumer_id,
                receive_id,
                expected_operation_id,
                minimum_delivery_count,
                ..
            } => verify_receive(
                scenario,
                index,
                consumer_id,
                receive_id,
                expected_operation_id,
                *minimum_delivery_count,
                violations,
            ),
            ScenarioAction::ShareAcknowledge {
                receive_id,
                acknowledgement_id,
                disposition,
                ..
            } => {
                let values = index.share_acknowledgements.get(acknowledgement_id);
                let matches = values.is_some_and(|values| {
                    values.len() == 1
                        && values[0].receive_id == *receive_id
                        && values[0].disposition == *disposition
                        && values[0].success
                        && values[0].delivery.is_none()
                        && values[0].code.is_none()
                });
                if !matches {
                    violations.push(violation(
                        "SHARE-003",
                        format!(
                            "share acknowledgement {acknowledgement_id} did not settle exactly once for batch {receive_id} with {disposition:?}"
                        ),
                        Some(acknowledgement_id.clone()),
                        values.into_iter().flatten().map(|value| {
                            format!("history:{}", value.history_sequence)
                        }).collect(),
                    ));
                }
            }
            ScenarioAction::DropShareBatch { receive_id, .. } => check_count(
                "SHARE-003",
                "share batch drop",
                index
                    .share_batches_dropped
                    .get(receive_id)
                    .map(Vec::as_slice),
                violations,
            ),
            ScenarioAction::CloseShareConsumer {
                consumer_id,
                expect_success,
            } => verify_close(index, consumer_id, *expect_success, violations),
            _ => {}
        }
    }
}

fn verify_receive(
    scenario: &Scenario,
    index: &HistoryIndex,
    consumer_id: &testlab_schema::ConsumerId,
    receive_id: &OperationId,
    expected_operation_id: &OperationId,
    minimum_delivery_count: i16,
    violations: &mut Vec<Violation>,
) {
    let receives = index.share_receives.get(receive_id);
    if receives.map_or(0, Vec::len) != 1 {
        violations.push(violation(
            "SHARE-002",
            format!(
                "share receive {receive_id} expected one completion, observed {}",
                receives.map_or(0, Vec::len)
            ),
            Some(receive_id.clone()),
            receive_references(receives.map(Vec::as_slice)),
        ));
        return;
    }
    let receive = &receives.map_or(&[][..], Vec::as_slice)[0];
    if &receive.consumer_id != consumer_id
        || receive.member_epoch.is_none_or(|epoch| epoch <= 0)
        || receive.assignment_epoch.is_none_or(|epoch| epoch == 0)
    {
        violations.push(violation(
            "SHARE-005",
            format!("share receive {receive_id} did not retain positive membership fences"),
            Some(receive_id.clone()),
            vec![format!("history:{}", receive.history_sequence)],
        ));
    }
    let Some(expected) = sent_record(scenario, expected_operation_id) else {
        return;
    };
    let matches = receive
        .records
        .iter()
        .filter(|record| {
            exact_record(&record.record, expected)
                && record.delivery_count >= minimum_delivery_count
        })
        .count();
    if receive.records.len() != 1 || matches != 1 {
        violations.push(violation(
            "SHARE-002",
            format!(
                "share receive {receive_id} expected one exact record from {expected_operation_id} with delivery count at least {minimum_delivery_count}"
            ),
            Some(receive_id.clone()),
            vec![format!("history:{}", receive.history_sequence)],
        ));
    }
}

fn verify_close(
    index: &HistoryIndex,
    consumer_id: &testlab_schema::ConsumerId,
    expect_success: bool,
    violations: &mut Vec<Violation>,
) {
    let closes = index.share_consumers_closed.get(consumer_id);
    let exact = closes.is_some_and(|values| values.len() == 1);
    if !exact {
        violations.push(violation(
            "SHARE-004",
            format!(
                "share consumer {consumer_id} expected one close terminal, observed {}",
                closes.map_or(0, Vec::len)
            ),
            None,
            closes
                .into_iter()
                .flatten()
                .map(|value| format!("history:{}", value.history_sequence))
                .collect(),
        ));
        return;
    }
    let close = &closes.map_or(&[][..], Vec::as_slice)[0];
    let coherent = if expect_success {
        close.success && close.delivery.is_none() && close.code.is_none()
    } else {
        !close.success
            && matches!(
                close.delivery,
                Some(TerminalStatus::DefinitelyNotSent | TerminalStatus::PossiblySent)
            )
            && close.code.is_some()
    };
    if !coherent {
        violations.push(violation(
            "SHARE-004",
            format!(
                "share consumer {consumer_id} close success={} delivery={:?} code={:?} contradicted expect_success={expect_success}",
                close.success, close.delivery, close.code
            ),
            None,
            vec![format!("history:{}", close.history_sequence)],
        ));
    }
}

fn check_count(
    contract: &str,
    label: &str,
    values: Option<&[u64]>,
    violations: &mut Vec<Violation>,
) {
    if values.map_or(0, <[u64]>::len) != 1 {
        violations.push(violation(
            contract,
            format!(
                "expected one {label}, observed {}",
                values.map_or(0, <[u64]>::len)
            ),
            None,
            references(values),
        ));
    }
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

fn receive_references(values: Option<&[IndexedShareReceive]>) -> Vec<String> {
    values
        .into_iter()
        .flatten()
        .map(|receive| format!("history:{}", receive.history_sequence))
        .collect()
}
