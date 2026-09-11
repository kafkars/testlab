//! Partition-reassignment actions translate without leaking expected broker state.

use testlab_schema::{
    AdapterCommand, AlterPartitionReassignmentsCommand, ListPartitionReassignmentsCommand,
    ScenarioAction,
};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    Some(match action {
        ScenarioAction::AlterPartitionReassignments(action) => (
            AdapterCommand::AlterPartitionReassignments(AlterPartitionReassignmentsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                changes: action.changes.clone(),
                allow_replication_factor_change: action.allow_replication_factor_change,
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::PartitionReassignmentsAltered(action.operation_id.clone()),
        ),
        ScenarioAction::ListPartitionReassignments(action) => (
            AdapterCommand::ListPartitionReassignments(ListPartitionReassignmentsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                targets: action.targets.clone(),
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::PartitionReassignmentsListed(action.operation_id.clone()),
        ),
        _ => return None,
    })
}
