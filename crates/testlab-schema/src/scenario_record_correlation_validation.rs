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
