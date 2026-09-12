//! Real-Kafka scenarios carry exact broker-visible correlation headers.

#[cfg(test)]
#[path = "scenario_record_correlation_validation_test.rs"]
mod tests;

use crate::{OperationId, RecordSpec, Scenario, ScenarioAction};

pub(crate) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    if !scenario.id.as_str().starts_with("kafka.") {
        return;
    }
    for step in &scenario.steps {
        for (operation_id, record) in records(&step.action) {
            if let Err(error) = record.validate_correlation(operation_id) {
                problems.push(format!(
                    "operation {operation_id} has invalid observer correlation: {error}"
                ));
            }
        }
        if let ScenarioAction::TransferAssignedRecord(action) = &step.action {
            validate_transfer(scenario, action, problems);
        }
    }
}

fn validate_transfer(
    scenario: &Scenario,
    action: &crate::AssignedRecordTransferAction,
    problems: &mut Vec<String>,
) {
    let Some(source) = scenario.steps.iter().find_map(|step| {
        records(&step.action)
            .into_iter()
            .find(|(operation_id, _)| *operation_id == &action.expected_input_operation_id)
            .map(|(_, record)| record)
    }) else {
        problems.push(format!(
            "assigned-record transfer {} references unsupported source operation {}",
            action.operation_id, action.expected_input_operation_id
        ));
        return;
    };
    if source
        .headers
        .iter()
        .any(|header| header.name == crate::RECORD_TRANSFER_OPERATION_HEADER)
    {
        problems.push(format!(
            "assigned-record transfer source {} already contains reserved header {}",
            action.expected_input_operation_id,
            crate::RECORD_TRANSFER_OPERATION_HEADER
        ));
    }
    let transferred = crate::transferred_record(
        source,
        &action.operation_id,
        &action.target_topic,
        action.target_partition,
    );
    if let Err(error) = transferred.validate() {
        problems.push(format!(
            "assigned-record transfer {} derives an invalid target record: {error}",
            action.operation_id
        ));
    }
}

fn records(action: &ScenarioAction) -> Vec<(&OperationId, &RecordSpec)> {
    match action {
        ScenarioAction::Send {
            operation_id,
            record,
            ..
        } => vec![(operation_id, record)],
        ScenarioAction::CancelProducerSend(action) => {
            vec![(&action.operation_id, &action.record)]
        }
        ScenarioAction::SendBatch { operations, .. }
        | ScenarioAction::ExecuteTransaction { operations, .. } => batch(operations),
        ScenarioAction::ExecuteTransactionalTransform(action) => batch(&action.operations),
        ScenarioAction::FenceTransaction { operation, .. } => {
            vec![(&operation.operation_id, &operation.record)]
        }
        ScenarioAction::StartConcurrentActors(action) => action
            .actors
            .iter()
            .filter_map(|actor| match actor {
                crate::ConcurrentActor::ProducerSend {
                    operation_id,
                    record,
                    ..
                } => Some((operation_id, record)),
                crate::ConcurrentActor::AssignedReceive { .. } => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn batch(operations: &[crate::BatchRecord]) -> Vec<(&OperationId, &RecordSpec)> {
    operations
        .iter()
        .map(|operation| (&operation.operation_id, &operation.record))
        .collect()
}
