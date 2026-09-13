//! CLI-backed Admin indexing separates public results from independent Kafka state.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminFeatureUpdatesValidation, AdminFeaturesDescription,
    AdminLogDirsDescription, AdminMetadataQuorumDescription, AdminProducersDescription,
    AdminProducersFenced, AdminReplicaLogDirsDescription, AdminTransactionsDescription,
    AdminTransactionsListing, BrokerFeaturesState, BrokerLogDirsState, BrokerMetadataQuorumState,
    BrokerProducersState, BrokerStateObservation, BrokerTransactionState, BrokerTransactionsState,
    OperationId, ScenarioAction,
};

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    match action {
        ScenarioAction::DescribeFeatures(value) => Some(&value.operation_id),
        ScenarioAction::DescribeProducers(value) => Some(&value.operation_id),
        ScenarioAction::DescribeLogDirs(value) => Some(&value.operation_id),
        ScenarioAction::DescribeReplicaLogDirs(value) => Some(&value.operation_id),
        ScenarioAction::DescribeMetadataQuorum(value) => Some(&value.operation_id),
        ScenarioAction::ListTransactions(value) => Some(&value.operation_id),
        ScenarioAction::DescribeTransactions(value) => Some(&value.operation_id),
        ScenarioAction::FenceProducers(value) => Some(&value.operation_id),
        _ => None,
    }
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    match command {
        AdapterCommand::DescribeFeatures(value) => Some(&value.operation_id),
        AdapterCommand::DescribeProducers(value) => Some(&value.operation_id),
        AdapterCommand::DescribeLogDirs(value) => Some(&value.operation_id),
        AdapterCommand::DescribeReplicaLogDirs(value) => Some(&value.operation_id),
        AdapterCommand::DescribeMetadataQuorum(value) => Some(&value.operation_id),
        AdapterCommand::ListTransactions(value) => Some(&value.operation_id),
        AdapterCommand::DescribeTransactions(value) => Some(&value.operation_id),
        AdapterCommand::FenceProducers(value) => Some(&value.operation_id),
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
                && action.broker_id == command.broker_id
                && action.timeout_ms == command.timeout_ms
        }
        (ScenarioAction::DescribeLogDirs(action), AdapterCommand::DescribeLogDirs(command)) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.topic == command.topic
                && action.partition == command.partition
                && action.timeout_ms == command.timeout_ms
        }
        (
            ScenarioAction::DescribeReplicaLogDirs(action),
            AdapterCommand::DescribeReplicaLogDirs(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.topic == command.topic
                && action.partition == command.partition
                && action.timeout_ms == command.timeout_ms
        }
        (
            ScenarioAction::DescribeMetadataQuorum(action),
            AdapterCommand::DescribeMetadataQuorum(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.timeout_ms == command.timeout_ms
        }
        (ScenarioAction::ListTransactions(action), AdapterCommand::ListTransactions(command)) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.state_filters == command.state_filters
                && action.producer_id_filters == command.producer_id_filters
                && action.duration_filter_ms == command.duration_filter_ms
                && action.transactional_id_pattern == command.transactional_id_pattern
                && action.timeout_ms == command.timeout_ms
        }
        (
            ScenarioAction::DescribeTransactions(action),
            AdapterCommand::DescribeTransactions(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action
                    .transactions
                    .iter()
                    .map(|transaction| &transaction.transactional_id)
                    .eq(&command.transactional_ids)
                && action.timeout_ms == command.timeout_ms
        }
        (ScenarioAction::FenceProducers(action), AdapterCommand::FenceProducers(command)) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action
                    .producers
                    .iter()
                    .map(|transaction| &transaction.transactional_id)
                    .eq(&command.transactional_ids)
                && action.timeout_ms == command.timeout_ms
        }
        (
            ScenarioAction::DescribeFeatures(_)
            | ScenarioAction::DescribeProducers(_)
            | ScenarioAction::DescribeLogDirs(_)
            | ScenarioAction::DescribeReplicaLogDirs(_)
            | ScenarioAction::DescribeMetadataQuorum(_)
            | ScenarioAction::ListTransactions(_)
            | ScenarioAction::DescribeTransactions(_)
            | ScenarioAction::FenceProducers(_),
            _,
        )
        | (
            _,
            AdapterCommand::DescribeFeatures(_)
            | AdapterCommand::DescribeProducers(_)
            | AdapterCommand::DescribeLogDirs(_)
            | AdapterCommand::DescribeReplicaLogDirs(_)
            | AdapterCommand::DescribeMetadataQuorum(_)
            | AdapterCommand::ListTransactions(_)
            | AdapterCommand::DescribeTransactions(_)
            | AdapterCommand::FenceProducers(_),
        ) => false,
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
    pub(crate) validations: BTreeMap<OperationId, Vec<Indexed<AdminFeatureUpdatesValidation>>>,
    pub(crate) observed: BTreeMap<OperationId, Vec<Indexed<BrokerFeaturesState>>>,
    pub(crate) producers_described: BTreeMap<OperationId, Vec<Indexed<AdminProducersDescription>>>,
    pub(crate) producers_observed: BTreeMap<OperationId, Vec<Indexed<BrokerProducersState>>>,
    pub(crate) log_dirs_described: BTreeMap<OperationId, Vec<Indexed<AdminLogDirsDescription>>>,
    pub(crate) log_dirs_observed: BTreeMap<OperationId, Vec<Indexed<BrokerLogDirsState>>>,
    pub(crate) replica_log_dirs_described:
        BTreeMap<OperationId, Vec<Indexed<AdminReplicaLogDirsDescription>>>,
    pub(crate) metadata_quorum_described:
        BTreeMap<OperationId, Vec<Indexed<AdminMetadataQuorumDescription>>>,
    pub(crate) metadata_quorum_observed:
        BTreeMap<OperationId, Vec<Indexed<BrokerMetadataQuorumState>>>,
    pub(crate) transactions_listed: BTreeMap<OperationId, Vec<Indexed<AdminTransactionsListing>>>,
    pub(crate) transactions_observed: BTreeMap<OperationId, Vec<Indexed<BrokerTransactionsState>>>,
    pub(crate) transactions_described:
        BTreeMap<OperationId, Vec<Indexed<AdminTransactionsDescription>>>,
    pub(crate) producers_fenced: BTreeMap<OperationId, Vec<Indexed<AdminProducersFenced>>>,
    pub(crate) transaction_states_observed:
        BTreeMap<OperationId, Vec<Indexed<BrokerTransactionState>>>,
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
            AdapterEvent::FeatureUpdatesValidated(value) => push(
                &mut self.validations,
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
            AdapterEvent::LogDirsDescribed(value) => push(
                &mut self.log_dirs_described,
                value.operation_id.clone(),
                value.clone(),
                sequence,
            ),
            AdapterEvent::ReplicaLogDirsDescribed(value) => push(
                &mut self.replica_log_dirs_described,
                value.operation_id.clone(),
                value.clone(),
                sequence,
            ),
            AdapterEvent::MetadataQuorumDescribed(value) => push(
                &mut self.metadata_quorum_described,
                value.operation_id.clone(),
                value.clone(),
                sequence,
            ),
            AdapterEvent::TransactionsListed(value) => push(
                &mut self.transactions_listed,
                value.operation_id.clone(),
                value.clone(),
                sequence,
            ),
            AdapterEvent::TransactionsDescribed(value) => push(
                &mut self.transactions_described,
                value.operation_id.clone(),
                value.clone(),
                sequence,
            ),
            AdapterEvent::ProducersFenced(value) => push(
                &mut self.producers_fenced,
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
            BrokerStateObservation::LogDirs(value) => push(
                &mut self.log_dirs_observed,
                value.operation_id.clone(),
                value.clone(),
                sequence,
            ),
            BrokerStateObservation::MetadataQuorum(value) => push(
                &mut self.metadata_quorum_observed,
                value.operation_id.clone(),
                value.clone(),
                sequence,
            ),
            BrokerStateObservation::Transactions(value) => push(
                &mut self.transactions_observed,
                value.operation_id.clone(),
                value.clone(),
                sequence,
            ),
            BrokerStateObservation::Transaction(value) => push(
                &mut self.transaction_states_observed,
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
