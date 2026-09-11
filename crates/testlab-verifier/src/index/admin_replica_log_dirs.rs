//! Replica log-directory alteration indexing keeps public completion distinct from CLI state.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminReplicaLogDirsAlteration, OperationId, ScenarioAction,
};

use super::admin_features::Indexed;

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    let ScenarioAction::AlterReplicaLogDirs(action) = action else {
        return None;
    };
    Some(&action.operation_id)
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    let AdapterCommand::AlterReplicaLogDirs(command) = command else {
        return None;
    };
    Some(&command.operation_id)
}

pub(super) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> Option<bool> {
    Some(match (action, command) {
        (
            ScenarioAction::AlterReplicaLogDirs(action),
            AdapterCommand::AlterReplicaLogDirs(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.assignments == command.assignments
                && action.timeout_ms == command.timeout_ms
        }
        (ScenarioAction::AlterReplicaLogDirs(_), _)
        | (_, AdapterCommand::AlterReplicaLogDirs(_)) => false,
        _ => return None,
    })
}

#[derive(Debug, Default)]
pub(crate) struct AdminReplicaLogDirsIndex {
    pub(crate) altered: BTreeMap<OperationId, Vec<Indexed<AdminReplicaLogDirsAlteration>>>,
}

impl AdminReplicaLogDirsIndex {
    pub(crate) fn record_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        let AdapterEvent::ReplicaLogDirsAltered(value) = event else {
            return false;
        };
        self.altered
            .entry(value.operation_id.clone())
            .or_default()
            .push(Indexed {
                history_sequence: sequence,
                value: value.clone(),
            });
        true
    }
}
