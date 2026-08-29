//! Record-admin scenario intent translates without exposing verifier watermarks.

use testlab_schema::{AdapterCommand, DeleteRecordsCommand, ScenarioAction};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    let ScenarioAction::DeleteRecords(action) = action else {
        return None;
    };
    Some((
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
    ))
}
