//! Feature indexing separates public Kafkars metadata from independent Kafka CLI state.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminFeaturesDescription, BrokerFeaturesState,
    BrokerStateObservation, OperationId, ScenarioAction,
};

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    match action {
        ScenarioAction::DescribeFeatures(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    match command {
        AdapterCommand::DescribeFeatures(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> Option<bool> {
    Some(match (action, command) {
        (ScenarioAction::DescribeFeatures(action), AdapterCommand::DescribeFeatures(command)) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.timeout_ms == command.timeout_ms
        }
        (ScenarioAction::DescribeFeatures(_), _) | (_, AdapterCommand::DescribeFeatures(_)) => {
            false
        }
        _ => return None,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Indexed<T> {
    pub(crate) history_sequence: u64,
    pub(crate) value: T,
}

#[derive(Debug, Default)]
pub(crate) struct AdminFeaturesIndex {
    pub(crate) described: BTreeMap<OperationId, Vec<Indexed<AdminFeaturesDescription>>>,
    pub(crate) observed: BTreeMap<OperationId, Vec<Indexed<BrokerFeaturesState>>>,
}

impl AdminFeaturesIndex {
    pub(crate) fn record_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        let AdapterEvent::FeaturesDescribed(value) = event else {
            return false;
        };
        self.described
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
        let BrokerStateObservation::Features(value) = observation else {
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
