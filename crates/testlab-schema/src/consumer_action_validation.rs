//! Assigned-consumer validation owns handle, assignment, and receive state.

use std::collections::BTreeMap;

use crate::{ClientId, ConsumerId, GroupOffsetReset, GroupProtocol, OperationId, ScenarioAction};

#[path = "consumer_expected_failure_validation.rs"]
mod expected_failure_validation;
#[path = "consumer_group_configuration_validation.rs"]
mod group_configuration_validation;
#[path = "consumer_group_static_validation.rs"]
mod static_validation;
#[path = "consumer_subscription_validation.rs"]
mod subscription_validation;

pub(crate) type ConsumerStates = BTreeMap<ConsumerId, ConsumerState>;

#[derive(Clone, Debug)]
pub(crate) struct ConsumerState {
    pub(crate) owner: ClientId,
    pub(crate) assigned: bool,
    pub(crate) closed: bool,
    pub(crate) group: Option<ConsumerGroupState>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConsumerGroupState {
    pub(crate) group_id: String,
    pub(crate) topics: Vec<String>,
    pub(crate) protocol: GroupProtocol,
    pub(crate) offset_reset: Option<GroupOffsetReset>,
    pub(crate) group_instance_id: Option<String>,
}

#[derive(Clone, Copy)]
pub(crate) struct ConsumerGroupInput<'a> {
    pub(crate) group_id: &'a str,
    pub(crate) topics: &'a [String],
    pub(crate) protocol: Option<GroupProtocol>,
    pub(crate) offset_reset: Option<GroupOffsetReset>,
    pub(crate) group_instance_id: Option<&'a str>,
}

pub(crate) fn validate(
    action: &ScenarioAction,
    state: &mut crate::scenario_action_validation::ActionStates,
    problems: &mut Vec<String>,
) {
    match action {
        ScenarioAction::CreateAssignedConsumer {
            client_id,
            consumer_id,
            ..
        } => create(
            client_id,
            consumer_id,
            &state.clients,
            &mut state.consumers,
            problems,
        ),
        ScenarioAction::AssignBeginning {
            consumer_id,
            topic,
            partition,
        } => assign(
            consumer_id,
            topic,
            *partition,
            &mut state.consumers,
            problems,
        ),
        ScenarioAction::AssignBeginningBatch(_)
        | ScenarioAction::ObserveGroupAssignments(_)
        | ScenarioAction::GroupReceiveSet(_) => {
            crate::consumer_group_ownership_validation::validate(action, state, problems);
        }
        ScenarioAction::ControlAssignedConsumer(_)
        | ScenarioAction::ControlGroupConsumer(_)
        | ScenarioAction::ShutdownGroupConsumer(_) => {
            crate::consumer_control_validation::validate(action, state, problems);
        }
        ScenarioAction::Receive {
            consumer_id,
            receive_id,
            expected_operation_id,
            timeout_ms,
            ..
        }
        | ScenarioAction::GroupReceive {
            consumer_id,
            receive_id,
            expected_operation_id,
            timeout_ms,
            ..
        } => crate::receive_action_validation::validate(
            consumer_id,
            receive_id,
            expected_operation_id,
            *timeout_ms,
            &mut state.consumers,
            &mut (&mut state.operation_ids, &mut state.sends),
            problems,
        ),
        ScenarioAction::CreateGroupConsumer {
            client_id,
            consumer_id,
            group_id,
            topics,
            protocol,
            configuration,
        } => {
            group_configuration_validation::validate(
                consumer_id,
                *protocol,
                configuration,
                problems,
            );
            create_group(
                client_id,
                consumer_id,
                ConsumerGroupInput {
                    group_id,
                    topics,
                    protocol: Some(*protocol),
                    offset_reset: Some(
                        configuration
                            .as_ref()
                            .map_or(GroupOffsetReset::Earliest, |value| value.offset_reset),
                    ),
                    group_instance_id: configuration
                        .as_ref()
                        .and_then(|value| value.group_instance_id.as_deref()),
                },
                &state.clients,
                &mut state.consumers,
                problems,
            );
        }
        ScenarioAction::CloseAssignedConsumer { consumer_id }
        | ScenarioAction::CloseGroupConsumer { consumer_id } => {
            close(consumer_id, &mut state.consumers, problems);
        }
        ScenarioAction::AbandonGroupConsumer(action) => {
            close(&action.consumer_id, &mut state.consumers, problems);
        }
        _ => {}
    }
    expected_failure_validation::validate(action, &state.consumers, problems);
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
                group: None,
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
    group: ConsumerGroupInput<'_>,
    clients: &BTreeMap<ClientId, bool>,
    consumers: &mut ConsumerStates,
    problems: &mut Vec<String>,
) {
    static_validation::validate(&group, consumers, consumer_id, problems);
    create(client_id, consumer_id, clients, consumers, problems);
    if let Some(state) = consumers.get_mut(consumer_id) {
        state.assigned = true;
        state.group = group.protocol.map(|protocol| ConsumerGroupState {
            group_id: group.group_id.to_owned(),
            topics: group.topics.to_owned(),
            protocol,
            offset_reset: group.offset_reset,
            group_instance_id: group.group_instance_id.map(str::to_owned),
        });
    }
    validate_name(consumer_id, "group", group.group_id, 255, problems);
    subscription_validation::validate(consumer_id, group.topics, problems);
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

pub(crate) fn open<'a>(
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

pub(super) fn validate_name(
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
