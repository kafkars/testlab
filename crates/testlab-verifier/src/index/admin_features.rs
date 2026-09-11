//! CLI-backed Admin indexing separates public results from independent Kafka state.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminFeaturesDescription, AdminProducersDescription,
    BrokerFeaturesState, BrokerProducersState, BrokerStateObservation, OperationId, ScenarioAction,
};

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    match action {
        ScenarioAction::DescribeFeatures(value) => Some(&value.operation_id),
        ScenarioAction::DescribeProducers(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    match command {
        AdapterCommand::DescribeFeatures(value) => Some(&value.operation_id),
        AdapterCommand::DescribeProducers(value) => Some(&value.operation_id),
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
        (ScenarioAction::DescribeProducers(action), AdapterCommand::DescribeProducers(command)) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.topic == command.topic
                && action.partition == command.partition
                && action.timeout_ms == command.timeout_ms
        }
        (ScenarioAction::DescribeFeatures(_), _) | (_, AdapterCommand::DescribeFeatures(_)) => {
            false
        }
        (ScenarioAction::DescribeProducers(_), _) | (_, AdapterCommand::DescribeProducers(_)) => {
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
    pub(crate) producers_described: BTreeMap<OperationId, Vec<Indexed<AdminProducersDescription>>>,
    pub(crate) producers_observed: BTreeMap<OperationId, Vec<Indexed<BrokerProducersState>>>,
}

impl AdminFeaturesIndex {
    pub(crate) fn record_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        match event {
            AdapterEvent::FeaturesDescribed(value) => push(
                &mut self.described,
                value.operation_id.clone(),
                value.clone(),
                sequence,
            ),
            AdapterEvent::ProducersDescribed(value) => push(
                &mut self.producers_described,
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
            BrokerStateObservation::Features(value) => push(
                &mut self.observed,
                value.operation_id.clone(),
                value.clone(),
                sequence,
            ),
            BrokerStateObservation::Producers(value) => push(
                &mut self.producers_observed,
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
