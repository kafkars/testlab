//! Composite Admin lifecycle indexing joins public results to independent CLI facts.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminBrokerUnregistration, AdminDelegationTokenLifecycle,
    AdminStreamsGroupAdminLifecycle, BrokerDelegationTokensState, BrokerStateObservation,
    BrokerStreamsGroupsState, OperationId, ScenarioAction,
};

pub(crate) use super::admin_client_quota::Indexed;

#[derive(Debug, Default)]
pub(crate) struct AdminLifecycleIndex {
    pub(crate) brokers_unregistered: BTreeMap<OperationId, Vec<Indexed<AdminBrokerUnregistration>>>,
    pub(crate) delegation_completed:
        BTreeMap<OperationId, Vec<Indexed<AdminDelegationTokenLifecycle>>>,
    pub(crate) delegation_observed:
        BTreeMap<OperationId, Vec<Indexed<BrokerDelegationTokensState>>>,
    pub(crate) streams_completed:
        BTreeMap<OperationId, Vec<Indexed<AdminStreamsGroupAdminLifecycle>>>,
    pub(crate) streams_observed: BTreeMap<OperationId, Vec<Indexed<BrokerStreamsGroupsState>>>,
}

impl AdminLifecycleIndex {
    pub(crate) fn record_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        match event {
            AdapterEvent::BrokerUnregistered(value) => self
                .brokers_unregistered
                .entry(value.operation_id.clone())
                .or_default()
                .push(Indexed {
                    history_sequence: sequence,
                    value: value.clone(),
                }),
            AdapterEvent::DelegationTokenLifecycleExercised(value) => self
                .delegation_completed
                .entry(value.operation_id.clone())
                .or_default()
                .push(Indexed {
                    history_sequence: sequence,
                    value: value.clone(),
                }),
            AdapterEvent::StreamsGroupAdminLifecycleExercised(value) => self
                .streams_completed
                .entry(value.operation_id.clone())
                .or_default()
                .push(Indexed {
                    history_sequence: sequence,
                    value: value.clone(),
                }),
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
            BrokerStateObservation::DelegationTokens(value) => self
                .delegation_observed
                .entry(value.operation_id.clone())
                .or_default()
                .push(Indexed {
                    history_sequence: sequence,
                    value: value.clone(),
                }),
            BrokerStateObservation::StreamsGroups(value) => self
                .streams_observed
                .entry(value.operation_id.clone())
                .or_default()
                .push(Indexed {
                    history_sequence: sequence,
                    value: value.clone(),
                }),
            _ => return false,
        }
        true
    }
}

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    match action {
        ScenarioAction::UnregisterBroker(value) => Some(&value.operation_id),
        ScenarioAction::ExerciseDelegationTokenLifecycle(value) => Some(&value.operation_id),
        ScenarioAction::ExerciseStreamsGroupAdminLifecycle(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    match command {
        AdapterCommand::UnregisterBroker(value) => Some(&value.operation_id),
        AdapterCommand::ExerciseDelegationTokenLifecycle(value) => Some(&value.operation_id),
        AdapterCommand::ExerciseStreamsGroupAdminLifecycle(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> Option<bool> {
    Some(match (action, command) {
        (ScenarioAction::UnregisterBroker(action), AdapterCommand::UnregisterBroker(command)) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.broker_id == command.broker_id
                && action.timeout_ms == command.timeout_ms
        }
        (
            ScenarioAction::ExerciseDelegationTokenLifecycle(action),
            AdapterCommand::ExerciseDelegationTokenLifecycle(command),
        ) => action == command,
        (
            ScenarioAction::ExerciseStreamsGroupAdminLifecycle(action),
            AdapterCommand::ExerciseStreamsGroupAdminLifecycle(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.primary_group_id == command.primary_group_id
                && action.secondary_group_id == command.secondary_group_id
                && action.input_topic == command.input_topic
                && action.altered_offset == command.altered_offset
                && action.timeout_ms == command.timeout_ms
        }
        (ScenarioAction::UnregisterBroker(_), _)
        | (_, AdapterCommand::UnregisterBroker(_))
        | (ScenarioAction::ExerciseDelegationTokenLifecycle(_), _)
        | (_, AdapterCommand::ExerciseDelegationTokenLifecycle(_))
        | (ScenarioAction::ExerciseStreamsGroupAdminLifecycle(_), _)
        | (_, AdapterCommand::ExerciseStreamsGroupAdminLifecycle(_)) => false,
        _ => return None,
    })
}
