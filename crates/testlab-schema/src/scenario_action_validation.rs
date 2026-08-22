//! Action validation owns handle state and producer operation identities.

use std::collections::{BTreeMap, BTreeSet};

use crate::consumer_action_validation::ConsumerStates;
use crate::{ClientId, OperationId, ProducerId, ScenarioAction};

type ClientStates = BTreeMap<ClientId, bool>;
type ProducerStates = BTreeMap<ProducerId, (ClientId, bool)>;
const MAX_BATCH_RECORDS: usize = 31;

pub(crate) fn validate_action(
    action: &ScenarioAction,
    clients: &mut ClientStates,
    producers: &mut ProducerStates,
    consumers: &mut ConsumerStates,
    operation_ids: &mut BTreeSet<OperationId>,
    sends: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    match action {
        ScenarioAction::CreateClient { client_id } => create_client(client_id, clients, problems),
        ScenarioAction::AwaitClientReady { client_id } => {
            require_live_client(client_id, clients, problems);
        }
        ScenarioAction::CreateProducer {
            client_id,
            producer_id,
        } => create_producer(client_id, producer_id, clients, producers, problems),
        ScenarioAction::SetBrokerBehavior { .. } => {}
        ScenarioAction::Send {
            producer_id,
            operation_id,
            record,
        } => {
            require_open_producer(producer_id, producers, problems);
            validate_operation(operation_id, record, operation_ids, sends, problems);
        }
        ScenarioAction::SendBatch {
            producer_id,
            operations: batch,
        } => {
            require_open_producer(producer_id, producers, problems);
            validate_batch(producer_id, batch, operation_ids, sends, problems);
        }
        ScenarioAction::CreateAssignedConsumer {
            client_id,
            consumer_id,
        } => crate::consumer_action_validation::create(
            client_id,
            consumer_id,
            clients,
            consumers,
            problems,
        ),
        ScenarioAction::AssignBeginning {
            consumer_id,
            topic,
            partition,
        } => crate::consumer_action_validation::assign(
            consumer_id,
            topic,
            *partition,
            consumers,
            problems,
        ),
        ScenarioAction::Receive {
            consumer_id,
            receive_id,
            expected_operation_id,
            timeout_ms,
        }
        | ScenarioAction::GroupReceive {
            consumer_id,
            receive_id,
            expected_operation_id,
            timeout_ms,
        } => crate::receive_action_validation::validate(
            consumer_id,
            receive_id,
            expected_operation_id,
            *timeout_ms,
            consumers,
            &mut (operation_ids, sends),
            problems,
        ),
        ScenarioAction::CreateGroupConsumer {
            client_id,
            consumer_id,
            group_id,
            topic,
        } => crate::consumer_action_validation::create_group(
            client_id,
            consumer_id,
            group_id,
            topic,
            clients,
            consumers,
            problems,
        ),
        ScenarioAction::CloseAssignedConsumer { consumer_id }
        | ScenarioAction::CloseGroupConsumer { consumer_id } => {
            crate::consumer_action_validation::close(consumer_id, consumers, problems);
        }
        action @ ScenarioAction::CreateTopic { .. } => {
            crate::admin_action_validation::validate(action, clients, operation_ids, problems);
        }
        ScenarioAction::Flush { producer_id } => {
            require_open_producer(producer_id, producers, problems);
        }
        ScenarioAction::CloseProducer { producer_id } => {
            close_producer(producer_id, producers, problems);
        }
        ScenarioAction::ShutdownClient { client_id } => {
            shutdown_client(client_id, clients, producers, consumers, problems);
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
    if producers
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

fn shutdown_client(
    client_id: &ClientId,
    clients: &mut ClientStates,
    producers: &ProducerStates,
    consumers: &ConsumerStates,
    problems: &mut Vec<String>,
) {
    let open = producers
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
    let open_consumers = crate::consumer_action_validation::open_for_client(consumers, client_id);
    if !open_consumers.is_empty() {
        problems.push(format!(
            "client {client_id} shut down with open consumers {}",
            open_consumers.join(", ")
        ));
    }
    match clients.get_mut(client_id) {
        Some(shutdown) if !*shutdown => *shutdown = true,
        Some(_) => {
            problems.push(format!("client {client_id} shut down more than once"));
        }
        None => problems.push(format!("missing client {client_id} was shut down")),
    }
}
