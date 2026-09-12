//! Producer record lookup joins every issued send-shaped action to its exact bytes.

use std::collections::BTreeMap;

use testlab_schema::{OperationId, RecordSpec, Scenario, ScenarioAction};

use crate::index::HistoryIndex;

pub(crate) fn issued(
    scenario: &Scenario,
    index: &HistoryIndex,
) -> BTreeMap<OperationId, RecordSpec> {
    let mut sends = BTreeMap::new();
    for step in &scenario.steps {
        if !index.action_issued(&step.action) {
            continue;
        }
        match &step.action {
            ScenarioAction::Send {
                operation_id,
                record,
                ..
            } => {
                sends.insert(operation_id.clone(), record.clone());
            }
            ScenarioAction::CancelProducerSend(action) => {
                sends.insert(action.operation_id.clone(), action.record.clone());
            }
            ScenarioAction::SendBatch { operations, .. }
            | ScenarioAction::ExecuteTransaction { operations, .. }
            | ScenarioAction::ExecuteTransactionalTransform(
                testlab_schema::TransactionalTransformAction { operations, .. },
            ) => sends.extend(
                operations
                    .iter()
                    .map(|operation| (operation.operation_id.clone(), operation.record.clone())),
            ),
            ScenarioAction::TransferAssignedRecord(action) => {
                if let Some(source) = sends.get(&action.expected_input_operation_id).cloned() {
                    sends.insert(
                        action.operation_id.clone(),
                        testlab_schema::transferred_record(
                            &source,
                            &action.operation_id,
                            &action.target_topic,
                            action.target_partition,
                        ),
                    );
                }
            }
            ScenarioAction::StartConcurrentActors(action) => {
                sends.extend(action.actors.iter().filter_map(|actor| match actor {
                    testlab_schema::ConcurrentActor::ProducerSend {
                        operation_id,
                        record,
                        ..
                    } => Some((operation_id.clone(), record.clone())),
                    testlab_schema::ConcurrentActor::AssignedReceive { .. } => None,
                }));
            }
            ScenarioAction::FenceTransaction { operation, .. } => {
                sends.insert(operation.operation_id.clone(), operation.record.clone());
            }
            _ => {}
        }
    }
    sends
}
