//! Partition-reassignment indexing keeps public and independent state separate.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminPartitionReassignmentsAlteration,
    AdminPartitionReassignmentsListing, BrokerPartitionAssignmentsState,
    BrokerPartitionReassignmentsState, BrokerStateObservation, OperationId, ScenarioAction,
};

use super::admin_features::Indexed;

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    match action {
        ScenarioAction::AlterPartitionReassignments(value) => Some(&value.operation_id),
        ScenarioAction::ListPartitionReassignments(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    match command {
        AdapterCommand::AlterPartitionReassignments(value) => Some(&value.operation_id),
        AdapterCommand::ListPartitionReassignments(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> Option<bool> {
    Some(match (action, command) {
        (
            ScenarioAction::AlterPartitionReassignments(action),
            AdapterCommand::AlterPartitionReassignments(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.changes == command.changes
                && action.allow_replication_factor_change == command.allow_replication_factor_change
                && action.timeout_ms == command.timeout_ms
        }
        (
            ScenarioAction::ListPartitionReassignments(action),
            AdapterCommand::ListPartitionReassignments(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.targets == command.targets
                && action.timeout_ms == command.timeout_ms
        }
        (ScenarioAction::AlterPartitionReassignments(_), _)
        | (_, AdapterCommand::AlterPartitionReassignments(_))
        | (ScenarioAction::ListPartitionReassignments(_), _)
        | (_, AdapterCommand::ListPartitionReassignments(_)) => false,
        _ => return None,
    })
}

#[derive(Debug, Default)]
pub(crate) struct AdminPartitionReassignmentsIndex {
    pub(crate) altered: BTreeMap<OperationId, Vec<Indexed<AdminPartitionReassignmentsAlteration>>>,
    pub(crate) listed: BTreeMap<OperationId, Vec<Indexed<AdminPartitionReassignmentsListing>>>,
    pub(crate) assignments_observed:
        BTreeMap<OperationId, Vec<Indexed<BrokerPartitionAssignmentsState>>>,
    pub(crate) reassignments_observed:
        BTreeMap<OperationId, Vec<Indexed<BrokerPartitionReassignmentsState>>>,
}

impl AdminPartitionReassignmentsIndex {
    pub(crate) fn record_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        match event {
            AdapterEvent::PartitionReassignmentsAltered(value) => push(
                &mut self.altered,
                value.operation_id.clone(),
                value.clone(),
                sequence,
            ),
            AdapterEvent::PartitionReassignmentsListed(value) => push(
                &mut self.listed,
                value.operation_id.clone(),
                value.clone(),
                sequence,
            ),
            _ => return false,
        }
        true
    }

    pub(crate) fn record_state(
        &mut self,
        observation: &BrokerStateObservation,
        sequence: u64,
    ) -> bool {
        match observation {
            BrokerStateObservation::PartitionAssignments(value) => push(
                &mut self.assignments_observed,
                value.operation_id.clone(),
                value.clone(),
                sequence,
            ),
            BrokerStateObservation::PartitionReassignments(value) => push(
                &mut self.reassignments_observed,
                value.operation_id.clone(),
                value.clone(),
                sequence,
            ),
            _ => return false,
        }
        true
    }
}

fn push<T>(
    index: &mut BTreeMap<OperationId, Vec<Indexed<T>>>,
    operation_id: OperationId,
    value: T,
    history_sequence: u64,
) {
    index.entry(operation_id).or_default().push(Indexed {
        history_sequence,
        value,
    });
}
