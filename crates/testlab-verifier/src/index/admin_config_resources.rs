//! Generic configuration-resource history retains exact commands and listings.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterCommand, AdapterEvent, BrokerConfigResourcesState, BrokerStateObservation, OperationId,
    ScenarioAction,
};

use super::admin_features::Indexed;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedConfigResourcesListing {
    pub(crate) history_sequence: u64,
    pub(crate) throttle_time_ms: u64,
    pub(crate) resources: Vec<testlab_schema::AdminConfigResource>,
}

#[derive(Debug, Default)]
pub(crate) struct AdminConfigResourcesIndex {
    pub(crate) listings: BTreeMap<OperationId, Vec<IndexedConfigResourcesListing>>,
    pub(crate) observed: BTreeMap<OperationId, Vec<Indexed<BrokerConfigResourcesState>>>,
}

impl AdminConfigResourcesIndex {
    pub(crate) fn record_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        let AdapterEvent::ConfigResourcesListed(value) = event else {
            return false;
        };
        self.listings
            .entry(value.operation_id.clone())
            .or_default()
            .push(IndexedConfigResourcesListing {
                history_sequence: sequence,
                throttle_time_ms: value.throttle_time_ms,
                resources: value.resources.clone(),
            });
        true
    }

    pub(crate) fn record_state(
        &mut self,
        observation: &BrokerStateObservation,
        sequence: u64,
    ) -> bool {
        let BrokerStateObservation::ConfigResources(value) = observation else {
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

pub(crate) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    match action {
        ScenarioAction::ListConfigResources(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(crate) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    match command {
        AdapterCommand::ListConfigResources(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(crate) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> Option<bool> {
    match (action, command) {
        (
            ScenarioAction::ListConfigResources(action),
            AdapterCommand::ListConfigResources(command),
        ) => Some(
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.api == command.api
                && action.timeout_ms == command.timeout_ms,
        ),
        _ => None,
    }
}
