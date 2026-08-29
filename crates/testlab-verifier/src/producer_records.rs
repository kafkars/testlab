//! Producer record lookup joins every issued send-shaped action to its exact bytes.

use std::collections::BTreeMap;

use testlab_schema::{OperationId, RecordSpec, Scenario, ScenarioAction};

use crate::index::HistoryIndex;

pub(crate) fn issued<'a>(
    scenario: &'a Scenario,
    index: &HistoryIndex,
) -> BTreeMap<OperationId, &'a RecordSpec> {
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
                sends.insert(operation_id.clone(), record);
            }
            ScenarioAction::CancelProducerSend(action) => {
                sends.insert(action.operation_id.clone(), &action.record);
            }
            ScenarioAction::SendBatch { operations, .. }
            | ScenarioAction::ExecuteTransaction { operations, .. }
            | ScenarioAction::ExecuteTransactionalTransform(
                testlab_schema::TransactionalTransformAction { operations, .. },
            ) => sends.extend(
                operations
                    .iter()
                    .map(|operation| (operation.operation_id.clone(), &operation.record)),
            ),
            ScenarioAction::StartConcurrentActors(action) => {
                sends.extend(action.actors.iter().filter_map(|actor| match actor {
                    testlab_schema::ConcurrentActor::ProducerSend {
                        operation_id,
                        record,
                        ..
                    } => Some((operation_id.clone(), record)),
                    testlab_schema::ConcurrentActor::AssignedReceive { .. } => None,
                }));
            }
            ScenarioAction::FenceTransaction { operation, .. } => {
                sends.insert(operation.operation_id.clone(), &operation.record);
            }
            _ => {}
        }
    }
    sends
}
