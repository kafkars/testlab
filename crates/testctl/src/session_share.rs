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
        expected_operation_id,
        minimum_delivery_count,
        ..
    } = action
    else {
        return None;
    };
    let AdapterEvent::ShareReceiveCompleted {
        receive_id: actual_receive,
        records,
        ..
    } = event
    else {
        return Some(false);
    };
    let expected = sent_record(scenario, expected_operation_id);
    Some(
        actual_receive == receive_id
            && expected
                .is_some_and(|expected| exact_receive(records, expected, *minimum_delivery_count)),
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
    expected: &RecordSpec,
    minimum_delivery_count: i16,
) -> bool {
    records.len() == 1
        && records[0].delivery_count >= minimum_delivery_count
        && exact_record(&records[0].record, expected)
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
