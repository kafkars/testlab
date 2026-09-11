//! Leader-election indexing keeps public results and independent metadata separate.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminLeaderElection, BrokerLeaderElectionState,
    BrokerStateObservation, OperationId, ScenarioAction,
};

use super::admin_features::Indexed;

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    let ScenarioAction::ElectLeaders(value) = action else {
        return None;
    };
    Some(&value.operation_id)
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    let AdapterCommand::ElectLeaders(value) = command else {
        return None;
    };
    Some(&value.operation_id)
}

pub(super) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> Option<bool> {
    Some(match (action, command) {
        (ScenarioAction::ElectLeaders(action), AdapterCommand::ElectLeaders(command)) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.election_type == command.election_type
                && action.targets == command.targets
                && action.timeout_ms == command.timeout_ms
        }
        (ScenarioAction::ElectLeaders(_), _) | (_, AdapterCommand::ElectLeaders(_)) => false,
        _ => return None,
    })
}

#[derive(Debug, Default)]
pub(crate) struct AdminLeaderElectionIndex {
    pub(crate) elected: BTreeMap<OperationId, Vec<Indexed<AdminLeaderElection>>>,
    pub(crate) observed: BTreeMap<OperationId, Vec<Indexed<BrokerLeaderElectionState>>>,
}

impl AdminLeaderElectionIndex {
    pub(crate) fn record_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        let AdapterEvent::LeadersElected(value) = event else {
            return false;
        };
        push(
            &mut self.elected,
            value.operation_id.clone(),
            value.clone(),
            sequence,
        );
        true
    }

    pub(crate) fn record_state(
        &mut self,
        observation: &BrokerStateObservation,
        sequence: u64,
    ) -> bool {
        let BrokerStateObservation::LeaderElection(value) = observation else {
            return false;
        };
        push(
            &mut self.observed,
            value.operation_id.clone(),
            value.clone(),
            sequence,
        );
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
