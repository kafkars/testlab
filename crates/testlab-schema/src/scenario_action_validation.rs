//! Action validation owns handle state and producer operation identities.

use std::collections::{BTreeMap, BTreeSet};

use crate::consumer_action_validation::ConsumerStates;
use crate::transaction_action_validation::{TransactionSends, TransactionStates};
use crate::{ClientId, OperationId, ProducerId, ScenarioAction};

pub(crate) type ClientStates = BTreeMap<ClientId, bool>;
pub(crate) type ProducerStates = BTreeMap<ProducerId, (ClientId, bool)>;
const MAX_BATCH_RECORDS: usize = 31;

#[derive(Default)]
pub(crate) struct ActionStates {
    pub(crate) clients: ClientStates,
    pub(crate) producers: ProducerStates,
    pub(crate) consumers: ConsumerStates,
    pub(crate) transactions: TransactionStates,
    pub(crate) operation_ids: BTreeSet<OperationId>,
    pub(crate) sends: BTreeSet<OperationId>,
    pub(crate) transaction_sends: TransactionSends,
    pub(crate) share_batches: crate::share_action_validation::ShareBatchStates,
    pub(crate) leader_disruptions: BTreeSet<(String, i32)>,
    pub(crate) stopped_brokers: BTreeSet<u16>,
}

pub(crate) fn validate_action(
    action: &ScenarioAction,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    match action {
        ScenarioAction::CreateClient { client_id } => {
            create_client(client_id, &mut state.clients, problems);
        }
        ScenarioAction::AwaitClientReady { client_id } => {
            require_live_client(client_id, &state.clients, problems);
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
        action @ (ScenarioAction::RestartBroker { .. }
        | ScenarioAction::StopBroker { .. }
        | ScenarioAction::StartBroker { .. }
        | ScenarioAction::StopPartitionLeader { .. }
        | ScenarioAction::RestorePartitionLeader { .. }) => {
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
        action @ (ScenarioAction::CreateAssignedConsumer { .. }
        | ScenarioAction::AssignBeginning { .. }
        | ScenarioAction::Receive { .. }
        | ScenarioAction::CloseAssignedConsumer { .. }
        | ScenarioAction::CreateGroupConsumer { .. }
        | ScenarioAction::GroupReceive { .. }
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
        action @ (ScenarioAction::CreateTopic { .. }
        | ScenarioAction::CreatePartitions(_)
        | ScenarioAction::DescribeTopic(_)
        | ScenarioAction::ListTopics(_)
        | ScenarioAction::ListOffsets(_)
        | ScenarioAction::ListConsumerGroupOffsets(_)) => {
            crate::admin_action_validation::validate(
                action,
                &state.clients,
                &mut state.operation_ids,
                problems,
            );
        }
        action @ (ScenarioAction::CreateTransactionalProducer { .. }
        | ScenarioAction::ExecuteTransaction { .. }
        | ScenarioAction::FenceTransaction { .. }
        | ScenarioAction::CloseTransactionalProducer { .. }) => {
            crate::transaction_action_validation::validate(action, state, problems);
        }
        ScenarioAction::Flush { producer_id } => {
            require_open_producer(producer_id, &state.producers, problems);
        }
        ScenarioAction::CloseProducer { producer_id } => {
            close_producer(producer_id, &mut state.producers, problems);
        }
        ScenarioAction::ShutdownClient { client_id } => shutdown_client(client_id, state, problems),
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

fn validate_operation(
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

fn require_open_producer(
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

fn shutdown_client(client_id: &ClientId, state: &mut ActionStates, problems: &mut Vec<String>) {
    let open = state
        .producers
        .iter()
        .filter(|(_, (owner, closed))| owner == client_id && !closed)
        .map(|(producer, _)| producer.to_string())
        .collect::<Vec<_>>();
    if !open.is_empty() {
        problems.push(format!(
            "client {client_id} shut down with open producers {}",
            open.join(", ")
        ));
    }
    let open_consumers =
        crate::consumer_action_validation::open_for_client(&state.consumers, client_id);
    if !open_consumers.is_empty() {
        problems.push(format!(
            "client {client_id} shut down with open consumers {}",
            open_consumers.join(", ")
        ));
    }
    let open_transactions =
        crate::transaction_action_validation::open_for_client(&state.transactions, client_id);
    if !open_transactions.is_empty() {
        problems.push(format!(
            "client {client_id} shut down with open transactional producers {}",
            open_transactions.join(", ")
        ));
    }
    match state.clients.get_mut(client_id) {
        Some(shutdown) if !*shutdown => *shutdown = true,
        Some(_) => {
            problems.push(format!("client {client_id} shut down more than once"));
        }
        None => problems.push(format!("missing client {client_id} was shut down")),
    }
}
