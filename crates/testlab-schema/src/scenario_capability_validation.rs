//! Capability validation binds used scenario actions to declared adapter support.

use std::collections::BTreeSet;

use crate::scenario_action_validation::ActionStates;
use crate::{Capability, GroupProtocol, Scenario, ScenarioAction};

pub(crate) fn record_usage(action: &ScenarioAction, usage: &mut BTreeSet<Capability>) {
    let capability = match action {
        ScenarioAction::SetBrokerBehavior { .. } => Some(Capability::ModelBroker),
        ScenarioAction::AwaitClientReady { .. } => Some(Capability::ClientReadiness),
        ScenarioAction::SendBatch { .. } => Some(Capability::ProducerBatch),
        ScenarioAction::CreateAssignedConsumer { .. }
        | ScenarioAction::AssignBeginning { .. }
        | ScenarioAction::Receive { .. }
        | ScenarioAction::CloseAssignedConsumer { .. } => Some(Capability::AssignedConsumer),
        ScenarioAction::CreateGroupConsumer { protocol, .. } => Some(match protocol {
            GroupProtocol::Classic => Capability::ConsumerGroups,
            GroupProtocol::Consumer => Capability::ConsumerProtocolGroups,
        }),
        ScenarioAction::CreateShareConsumer { .. }
        | ScenarioAction::ShareReceive { .. }
        | ScenarioAction::ShareAcknowledge { .. }
        | ScenarioAction::DropShareBatch { .. }
        | ScenarioAction::CloseShareConsumer { .. } => Some(Capability::ShareConsumer),
        ScenarioAction::CreateTopic { .. }
        | ScenarioAction::CreatePartitions(_)
        | ScenarioAction::DescribeTopic(_)
        | ScenarioAction::ListTopics(_)
        | ScenarioAction::ListOffsets(_) => Some(Capability::Admin),
        ScenarioAction::CreateTransactionalProducer { .. }
        | ScenarioAction::ExecuteTransaction { .. }
        | ScenarioAction::FenceTransaction { .. }
        | ScenarioAction::CloseTransactionalProducer { .. } => Some(Capability::Transactions),
        _ => None,
    };
    usage.extend(capability);
}

pub(crate) fn validate_required(
    scenario: &Scenario,
    usage: &BTreeSet<Capability>,
    state: &ActionStates,
    problems: &mut Vec<String>,
) {
    require(
        !state.sends.is_empty(),
        Capability::Producer,
        "send steps require the producer capability",
        scenario,
        problems,
    );
    for (used, capability, message) in [
        (
            usage.contains(&Capability::ProducerBatch),
            Capability::ProducerBatch,
            "batch-send steps require the producer_batch capability",
        ),
        (
            usage.contains(&Capability::AssignedConsumer),
            Capability::AssignedConsumer,
            "assigned-consumer steps require the assigned_consumer capability",
        ),
        (
            usage.contains(&Capability::ConsumerGroups),
            Capability::ConsumerGroups,
            "classic group-consumer steps require the consumer_groups capability",
        ),
        (
            usage.contains(&Capability::ConsumerProtocolGroups),
            Capability::ConsumerProtocolGroups,
            "KIP-848 group-consumer steps require the consumer_protocol_groups capability",
        ),
        (
            usage.contains(&Capability::ShareConsumer),
            Capability::ShareConsumer,
            "share-consumer steps require the share_consumer capability",
        ),
        (
            usage.contains(&Capability::Admin),
            Capability::Admin,
            "admin steps require the admin capability",
        ),
        (
            usage.contains(&Capability::Transactions),
            Capability::Transactions,
            "transaction steps require the transactions capability",
        ),
        (
            usage.contains(&Capability::ModelBroker),
            Capability::ModelBroker,
            "broker-control steps require the model_broker capability",
        ),
        (
            usage.contains(&Capability::ClientReadiness),
            Capability::ClientReadiness,
            "client readiness steps require the client_readiness capability",
        ),
    ] {
        require(used, capability, message, scenario, problems);
    }
    let handles = !state.clients.is_empty()
        || !state.producers.is_empty()
        || !state.consumers.is_empty()
        || !state.transactions.is_empty();
    require(
        handles,
        Capability::Lifecycle,
        "handle steps require the lifecycle capability",
        scenario,
        problems,
    );
}

fn require(
    used: bool,
    capability: Capability,
    message: &str,
    scenario: &Scenario,
    problems: &mut Vec<String>,
) {
    if used && !scenario.requires.contains(&capability) {
        problems.push(message.to_owned());
    }
}
