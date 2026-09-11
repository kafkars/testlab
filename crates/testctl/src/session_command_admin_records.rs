//! Record-admin scenario intent translates without exposing verifier watermarks.

use testlab_schema::{
    AdapterCommand, DeleteRecordsBatchCommand, DeleteRecordsBatchSelection, DeleteRecordsCommand,
    ScenarioAction,
};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    Some(match action {
        ScenarioAction::DeleteRecords(action) => (
            AdapterCommand::DeleteRecords(DeleteRecordsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                before_offset: action.before_offset,
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::RecordsDeleted {
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
            },
        ),
        ScenarioAction::DeleteRecordsBatch(action) => (
            AdapterCommand::DeleteRecordsBatch(DeleteRecordsBatchCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                targets: action
                    .targets
                    .iter()
                    .map(|target| DeleteRecordsBatchSelection {
                        topic: target.topic.clone(),
                        partition: target.partition,
                        boundary: target.boundary,
                    })
                    .collect(),
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::RecordsBatchDeleted {
                operation_id: action.operation_id.clone(),
                targets: action
                    .targets
                    .iter()
                    .map(|target| (target.topic.clone(), target.partition))
                    .collect(),
            },
        ),
        _ => return None,
    })
}
