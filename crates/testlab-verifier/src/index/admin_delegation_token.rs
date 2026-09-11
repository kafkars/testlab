//! Delegation-token indexing joins public non-secret facts to CLI absence.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminDelegationTokenLifecycle, BrokerDelegationTokensState,
    BrokerStateObservation, OperationId, ScenarioAction,
};

pub(crate) use super::admin_client_quota::Indexed;

#[derive(Debug, Default)]
pub(crate) struct AdminDelegationTokenIndex {
    pub(crate) completed: BTreeMap<OperationId, Vec<Indexed<AdminDelegationTokenLifecycle>>>,
    pub(crate) observed: BTreeMap<OperationId, Vec<Indexed<BrokerDelegationTokensState>>>,
}

impl AdminDelegationTokenIndex {
    pub(crate) fn record_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        let AdapterEvent::DelegationTokenLifecycleExercised(value) = event else {
            return false;
        };
        self.completed
            .entry(value.operation_id.clone())
            .or_default()
            .push(Indexed {
                history_sequence: sequence,
                value: value.clone(),
            });
        true
    }

    pub(crate) fn record_state(
        &mut self,
        observation: &BrokerStateObservation,
        sequence: u64,
    ) -> bool {
        let BrokerStateObservation::DelegationTokens(value) = observation else {
            return false;
        };
        self.observed
            .entry(value.operation_id.clone())
            .or_default()
            .push(Indexed {
                history_sequence: sequence,
                value: value.clone(),
            });
        true
    }
}

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    match action {
        ScenarioAction::ExerciseDelegationTokenLifecycle(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    match command {
        AdapterCommand::ExerciseDelegationTokenLifecycle(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> Option<bool> {
    Some(match (action, command) {
        (
            ScenarioAction::ExerciseDelegationTokenLifecycle(action),
            AdapterCommand::ExerciseDelegationTokenLifecycle(command),
        ) => action == command,
        (ScenarioAction::ExerciseDelegationTokenLifecycle(_), _)
        | (_, AdapterCommand::ExerciseDelegationTokenLifecycle(_)) => false,
        _ => return None,
    })
}
