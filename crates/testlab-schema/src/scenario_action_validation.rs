//! Action validation owns handle state and producer operation identities.

use std::collections::BTreeSet;

pub(crate) use crate::scenario_action_state::{ActionStates, ClientStates, ProducerStates};
use crate::transaction_action_validation::TransactionStates;
use crate::{ClientId, OperationId, ProducerId, ScenarioAction};

const MAX_BATCH_RECORDS: usize = 31;

#[allow(clippy::too_many_lines, reason = "exhaustive action routing")]
pub(crate) fn validate_action(
    action: &ScenarioAction,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    if let Some(active) = crate::concurrent_validation::active_id(state)
        && !crate::concurrent_validation::allowed_while_active(action)
    {
        problems.push(format!(
            "action cannot run while concurrent group {active} is active"
        ));
        return;
    }
    match action {
        ScenarioAction::CreateClient { client_id } => {
            create_client(client_id, &mut state.clients, problems);
        }
        ScenarioAction::CreateConfiguredClient(action) => {
            crate::producer_configuration_validation::validate(&action.configuration, problems);
            create_client(&action.client_id, &mut state.clients, problems);
        }
        ScenarioAction::AwaitClientReady { client_id } => {
            require_live_client(client_id, &state.clients, problems);
        }
        ScenarioAction::ObserveClientMetrics(action) => {
            require_live_client(&action.client_id, &state.clients, problems);
            if !state.operation_ids.insert(action.operation_id.clone()) {
                problems.push(format!("duplicate operation id {}", action.operation_id));
            }
        }
        ScenarioAction::CreateProducer {
            client_id,
            producer_id,
        } => create_producer(
            client_id,
            producer_id,
            &state.clients,
            &mut state.producers,
            &state.transactions,
            problems,
        ),
        ScenarioAction::SetBrokerBehavior { .. } => {}
        action @ (ScenarioAction::ArmProtocolFault(_)
        | ScenarioAction::AlterNetworkFault(_)
        | ScenarioAction::CutNetworkConnections(_)
        | ScenarioAction::RestartBroker { .. }
        | ScenarioAction::StopBroker { .. }
        | ScenarioAction::StartBroker { .. }
        | ScenarioAction::StopBrokerRole { .. }
        | ScenarioAction::RestoreBrokerRole { .. }
        | ScenarioAction::AlterBrokerPolicy(_)) => {
            crate::scenario_environment_action_validation::validate(action, state, problems);
        }
        ScenarioAction::Send {
            producer_id,
            operation_id,
            record,
        } => {
            require_open_producer(producer_id, &state.producers, problems);
            validate_operation(
                operation_id,
                record,
                &mut state.operation_ids,
                &mut state.sends,
                problems,
            );
        }
        ScenarioAction::CancelProducerSend(action) => {
            crate::producer_cancellation_validation::validate(action, state, problems);
        }
        ScenarioAction::SendBatch {
            producer_id,
            operations: batch,
        } => {
            require_open_producer(producer_id, &state.producers, problems);
            validate_batch(
                producer_id,
                batch,
                &mut state.operation_ids,
                &mut state.sends,
                problems,
            );
        }
        ScenarioAction::StartConcurrentActors(_) | ScenarioAction::JoinConcurrentActors(_) => {
            crate::concurrent_validation::validate(action, state, problems);
        }
        action @ (ScenarioAction::CreateAssignedConsumer { .. }
        | ScenarioAction::AssignBeginning { .. }
        | ScenarioAction::AssignBeginningBatch(_)
        | ScenarioAction::ControlAssignedConsumer(_)
        | ScenarioAction::Receive { .. }
        | ScenarioAction::CloseAssignedConsumer { .. }
        | ScenarioAction::CreateGroupConsumer { .. }
        | ScenarioAction::GroupReceive { .. }
        | ScenarioAction::ObserveGroupAssignments(_)
        | ScenarioAction::GroupReceiveSet(_)
        | ScenarioAction::ControlGroupConsumer(_)
        | ScenarioAction::ShutdownGroupConsumer(_)
        | ScenarioAction::CloseGroupConsumer { .. }) => {
            crate::consumer_action_validation::validate(action, state, problems);
        }
        action @ (ScenarioAction::CreateShareConsumer { .. }
        | ScenarioAction::ShareReceive { .. }
        | ScenarioAction::ShareAcknowledge { .. }
        | ScenarioAction::DropShareBatch { .. }
        | ScenarioAction::CloseShareConsumer { .. }) => {
            crate::share_action_validation::validate(action, state, problems);
        }
        action @ (ScenarioAction::CreateTopic(_)
        | ScenarioAction::CreateTopicsBatch(_)
        | ScenarioAction::CreatePartitions(_)
        | ScenarioAction::DeleteTopic(_)
        | ScenarioAction::DescribeTopic(_)
        | ScenarioAction::ListTopics(_)
        | ScenarioAction::ListOffsets(_)
        | ScenarioAction::DeleteRecords(_)
        | ScenarioAction::DescribeTopicConfig(_)
        | ScenarioAction::AlterTopicConfig(_)
        | ScenarioAction::DescribeCluster(_)
        | ScenarioAction::ListConsumerGroups(_)
        | ScenarioAction::DescribeConsumerGroup(_)
        | ScenarioAction::ListConsumerGroupOffsets(_)
        | ScenarioAction::ListConsumerGroupOffsetsBatch(_)
        | ScenarioAction::ListConsumerGroupsOffsets(_)
        | ScenarioAction::AlterConsumerGroupOffset(_)
        | ScenarioAction::AlterConsumerGroupOffsets(_)
        | ScenarioAction::DeleteConsumerGroupOffset(_)
        | ScenarioAction::DeleteConsumerGroupOffsets(_)
        | ScenarioAction::DeleteConsumerGroup(_)
        | ScenarioAction::DescribeClassicGroups(_)) => {
            crate::admin_action_validation::validate(
                action,
                &state.clients,
                &mut state.operation_ids,
                problems,
            );
        }
        action @ (ScenarioAction::CreateTransactionalProducer { .. }
        | ScenarioAction::ExecuteTransaction { .. }
        | ScenarioAction::ExecuteTransactionalTransform(_)
        | ScenarioAction::FenceTransaction { .. }
        | ScenarioAction::CloseTransactionalProducer(_)) => {
            crate::transaction_action_validation::validate(action, state, problems);
        }
        ScenarioAction::Flush { producer_id } => {
            require_open_producer(producer_id, &state.producers, problems);
        }
        ScenarioAction::CloseProducer { producer_id } => {
            close_producer(producer_id, &mut state.producers, problems);
        }
        ScenarioAction::ShutdownClient { client_id } => {
            crate::scenario_action_lifecycle_validation::shutdown_client(
                client_id, state, problems,
            );
        }
    }
}

fn create_client(client_id: &ClientId, clients: &mut ClientStates, problems: &mut Vec<String>) {
    if clients.insert(client_id.clone(), false).is_some() {
        problems.push(format!("duplicate client id {client_id}"));
    }
}

fn validate_batch(
    producer_id: &ProducerId,
    batch: &[crate::BatchRecord],
    operation_ids: &mut BTreeSet<OperationId>,
    sends: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    if batch.is_empty() {
        problems.push(format!("producer {producer_id} received an empty batch"));
    }
    if batch.len() > MAX_BATCH_RECORDS {
        problems.push(format!(
            "producer {producer_id} batch has {} records, maximum is {MAX_BATCH_RECORDS}",
            batch.len()
        ));
    }
    for operation in batch {
        validate_operation(
            &operation.operation_id,
            &operation.record,
            operation_ids,
            sends,
            problems,
        );
    }
}

pub(crate) fn validate_operation(
    operation_id: &OperationId,
    record: &crate::RecordSpec,
    operation_ids: &mut BTreeSet<OperationId>,
    sends: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    if !operation_ids.insert(operation_id.clone()) {
        problems.push(format!("duplicate operation id {operation_id}"));
    }
    sends.insert(operation_id.clone());
    if let Err(error) = record.validate() {
        problems.push(format!(
            "operation {operation_id} has invalid record: {error}"
        ));
    }
}

fn require_live_client(client_id: &ClientId, clients: &ClientStates, problems: &mut Vec<String>) {
    match clients.get(client_id) {
        Some(false) => {}
        Some(true) => problems.push(format!("client {client_id} was used after shutdown")),
        None => problems.push(format!("missing client {client_id} was used")),
    }
}

fn create_producer(
    client_id: &ClientId,
    producer_id: &ProducerId,
    clients: &ClientStates,
    producers: &mut ProducerStates,
    transactions: &TransactionStates,
    problems: &mut Vec<String>,
) {
    match clients.get(client_id) {
        Some(false) => {}
        Some(true) => problems.push(format!(
            "producer {producer_id} uses shut down client {client_id}"
        )),
        None => problems.push(format!(
            "producer {producer_id} uses missing client {client_id}"
        )),
    }
    if transactions.contains_key(producer_id)
        || producers
            .insert(producer_id.clone(), (client_id.clone(), false))
            .is_some()
    {
        problems.push(format!("duplicate producer id {producer_id}"));
    }
}

pub(crate) fn require_open_producer(
    producer_id: &ProducerId,
    producers: &ProducerStates,
    problems: &mut Vec<String>,
) {
    match producers.get(producer_id) {
        Some((_, false)) => {}
        Some((_, true)) => {
            problems.push(format!("producer {producer_id} was used after close"));
        }
        None => problems.push(format!("missing producer {producer_id} was used")),
    }
}

fn close_producer(
    producer_id: &ProducerId,
    producers: &mut ProducerStates,
    problems: &mut Vec<String>,
) {
    match producers.get_mut(producer_id) {
        Some((_, closed)) if !*closed => *closed = true,
        Some(_) => {
            problems.push(format!("producer {producer_id} closed more than once"));
        }
        None => problems.push(format!("missing producer {producer_id} was closed")),
    }
}
