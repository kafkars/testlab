//! Assigned-consumer validation owns handle, assignment, and receive state.

use std::collections::BTreeMap;

use crate::{ClientId, ConsumerId, OperationId};

pub(crate) type ConsumerStates = BTreeMap<ConsumerId, ConsumerState>;

#[derive(Clone, Debug)]
pub(crate) struct ConsumerState {
    owner: ClientId,
    assigned: bool,
    closed: bool,
}

pub(crate) fn create(
    client_id: &ClientId,
    consumer_id: &ConsumerId,
    clients: &BTreeMap<ClientId, bool>,
    consumers: &mut ConsumerStates,
    problems: &mut Vec<String>,
) {
    match clients.get(client_id) {
        Some(false) => {}
        Some(true) => problems.push(format!(
            "consumer {consumer_id} uses shut down client {client_id}"
        )),
        None => problems.push(format!(
            "consumer {consumer_id} uses missing client {client_id}"
        )),
    }
    if consumers
        .insert(
            consumer_id.clone(),
            ConsumerState {
                owner: client_id.clone(),
                assigned: false,
                closed: false,
            },
        )
        .is_some()
    {
        problems.push(format!("duplicate consumer id {consumer_id}"));
    }
}

pub(crate) fn assign(
    consumer_id: &ConsumerId,
    topic: &str,
    partition: i32,
    consumers: &mut ConsumerStates,
    problems: &mut Vec<String>,
) {
    let Some(state) = open(consumer_id, consumers, problems) else {
        return;
    };
    state.assigned = true;
    if topic.is_empty() || topic.len() > 249 {
        problems.push(format!("consumer {consumer_id} has invalid topic"));
    }
    if partition < 0 {
        problems.push(format!(
            "consumer {consumer_id} has negative partition {partition}"
        ));
    }
}

pub(crate) fn create_group(
    client_id: &ClientId,
    consumer_id: &ConsumerId,
    group_id: &str,
    topic: &str,
    clients: &BTreeMap<ClientId, bool>,
    consumers: &mut ConsumerStates,
    problems: &mut Vec<String>,
) {
    create(client_id, consumer_id, clients, consumers, problems);
    if let Some(state) = consumers.get_mut(consumer_id) {
        state.assigned = true;
    }
    validate_name(consumer_id, "group", group_id, 255, problems);
    validate_name(consumer_id, "topic", topic, 249, problems);
}

pub(crate) fn receive(
    consumer_id: &ConsumerId,
    receive_id: &OperationId,
    timeout_ms: u64,
    consumers: &mut ConsumerStates,
    problems: &mut Vec<String>,
) {
    if let Some(state) = open(consumer_id, consumers, problems)
        && !state.assigned
    {
        problems.push(format!("consumer {consumer_id} received before assignment"));
    }
    if !(100..=60_000).contains(&timeout_ms) {
        problems.push(format!(
            "receive {receive_id} timeout_ms must be between 100 and 60000"
        ));
    }
}

pub(crate) fn close(
    consumer_id: &ConsumerId,
    consumers: &mut ConsumerStates,
    problems: &mut Vec<String>,
) {
    if let Some(state) = open(consumer_id, consumers, problems) {
        state.closed = true;
    }
}

pub(crate) fn open_for_client(consumers: &ConsumerStates, client_id: &ClientId) -> Vec<String> {
    consumers
        .iter()
        .filter(|(_, state)| &state.owner == client_id && !state.closed)
        .map(|(consumer, _)| consumer.to_string())
        .collect()
}

pub(crate) fn unclosed(consumers: ConsumerStates) -> Vec<ConsumerId> {
    consumers
        .into_iter()
        .filter_map(|(consumer, state)| (!state.closed).then_some(consumer))
        .collect()
}

fn open<'a>(
    consumer_id: &ConsumerId,
    consumers: &'a mut ConsumerStates,
    problems: &mut Vec<String>,
) -> Option<&'a mut ConsumerState> {
    match consumers.get_mut(consumer_id) {
        Some(state) if !state.closed => Some(state),
        Some(_) => {
            problems.push(format!("consumer {consumer_id} was used after close"));
            None
        }
        None => {
            problems.push(format!("missing consumer {consumer_id} was used"));
            None
        }
    }
}

fn validate_name(
    consumer_id: &ConsumerId,
    kind: &str,
    value: &str,
    maximum: usize,
    problems: &mut Vec<String>,
) {
    if value.is_empty() || value.len() > maximum {
        problems.push(format!("consumer {consumer_id} has invalid {kind}"));
    }
}
