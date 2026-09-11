//! Reassignment targets preserve exact scenario-to-wire correlation.

use testlab_schema::{
    AdapterCommand, AlterPartitionReassignmentsCommand, ListPartitionReassignmentsCommand,
    OperationId, PartitionReassignmentChangeSpec, ScenarioAction,
};

use crate::observer_admin_target::{AdminTarget, TargetMatch};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PartitionAssignmentsTarget {
    pub(super) operation_id: OperationId,
    pub(super) assignments: Vec<PartitionReassignmentChangeSpec>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PartitionReassignmentsTarget {
    pub(super) operation_id: OperationId,
}

pub(super) fn match_action(action: &ScenarioAction) -> Option<TargetMatch> {
    Some(match action {
        ScenarioAction::AlterPartitionReassignments(action) => (
            AdapterCommand::AlterPartitionReassignments(AlterPartitionReassignmentsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                changes: action.changes.clone(),
                allow_replication_factor_change: action.allow_replication_factor_change,
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::PartitionAssignments(PartitionAssignmentsTarget {
                operation_id: action.operation_id.clone(),
                assignments: action.changes.clone(),
            }),
        ),
        ScenarioAction::ListPartitionReassignments(action) => (
            AdapterCommand::ListPartitionReassignments(ListPartitionReassignmentsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                targets: action.targets.clone(),
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::PartitionReassignments(PartitionReassignmentsTarget {
                operation_id: action.operation_id.clone(),
            }),
        ),
        _ => return None,
    })
}
