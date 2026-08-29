//! Share step settlement stops before dependent commands when a delivery is missing or wrong.

use testlab_schema::{
    AdapterEvent, ByteString, ConsumedRecord, OperationId, RecordSpec, Scenario, ScenarioAction,
    ShareConsumedRecord,
};

pub(crate) fn receive_succeeded(
    scenario: &Scenario,
    action: &ScenarioAction,
    event: &AdapterEvent,
) -> Option<bool> {
    let ScenarioAction::ShareReceive {
        receive_id,
        expected_operation_ids,
        minimum_delivery_count,
        expected_acquisition_count,
        ..
    } = action
    else {
        return None;
    };
    let AdapterEvent::ShareReceiveCompleted {
        receive_id: actual_receive,
        records,
        acquisition_count,
        ..
    } = event
    else {
        return Some(false);
    };
    let expected = expected_operation_ids
        .iter()
        .map(|operation_id| sent_record(scenario, operation_id))
        .collect::<Option<Vec<_>>>();
    Some(
        actual_receive == receive_id
            && expected_acquisition_count.is_none_or(|expected| *acquisition_count == expected)
            && expected
                .is_some_and(|expected| exact_receive(records, &expected, *minimum_delivery_count)),
    )
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

fn exact_receive(
    records: &[ShareConsumedRecord],
    expected: &[&RecordSpec],
    minimum_delivery_count: i16,
) -> bool {
    records.len() == expected.len()
        && records.iter().zip(expected).all(|(record, expected)| {
            record.delivery_count >= minimum_delivery_count
                && exact_record(&record.record, expected)
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
