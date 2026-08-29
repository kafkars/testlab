//! Capability validation binds used scenario actions to declared adapter support.

use std::collections::BTreeSet;

use crate::scenario_action_validation::ActionStates;
use crate::{Capability, GroupProtocol, Scenario, ScenarioAction};

pub(crate) fn record_usage(action: &ScenarioAction, usage: &mut BTreeSet<Capability>) {
    if let ScenarioAction::StartConcurrentActors(action) = action {
        usage.insert(Capability::ConcurrentActors);
        for actor in &action.actors {
            match actor {
                crate::ConcurrentActor::ProducerSend { .. } => {
                    usage.insert(Capability::Producer);
                }
                crate::ConcurrentActor::AssignedReceive { .. } => {
                    usage.insert(Capability::AssignedConsumer);
                }
            }
        }
        return;
    }
    if matches!(action, ScenarioAction::JoinConcurrentActors(_)) {
        usage.insert(Capability::ConcurrentActors);
        return;
    }
    let capability = match action {
        ScenarioAction::SetBrokerBehavior { .. } => Some(Capability::ModelBroker),
        ScenarioAction::AwaitClientReady { .. } => Some(Capability::ClientReadiness),
        ScenarioAction::ObserveClientMetrics(_) => Some(Capability::ClientMetrics),
        ScenarioAction::CreateConfiguredClient(_) => Some(Capability::ProducerConfiguration),
        ScenarioAction::CancelProducerSend(_) => Some(Capability::ProducerCancellation),
        ScenarioAction::ControlAssignedConsumer(_) => Some(Capability::AssignedConsumerControls),
        ScenarioAction::ControlGroupConsumer(_) => Some(Capability::GroupConsumerControls),
        ScenarioAction::ShutdownGroupConsumer(_) => Some(Capability::GroupConsumerShutdown),
        ScenarioAction::SendBatch { .. } => Some(Capability::ProducerBatch),
        ScenarioAction::CreateAssignedConsumer { .. }
        | ScenarioAction::AssignBeginning { .. }
        | ScenarioAction::AssignBeginningBatch(_)
        | ScenarioAction::Receive { .. }
        | ScenarioAction::CloseAssignedConsumer { .. } => Some(Capability::AssignedConsumer),
        ScenarioAction::CreateGroupConsumer {
            protocol,
            configuration,
            ..
        } => {
            if configuration.is_some() {
                usage.insert(Capability::GroupConsumerConfiguration);
            }
            Some(match protocol {
                GroupProtocol::Classic => Capability::ConsumerGroups,
                GroupProtocol::Consumer => Capability::ConsumerProtocolGroups,
            })
        }
        ScenarioAction::CreateShareConsumer { configuration, .. } => {
            if configuration.is_some() {
                usage.insert(Capability::ShareConsumerConfiguration);
            }
            Some(Capability::ShareConsumer)
        }
        ScenarioAction::ShareReceive { .. }
        | ScenarioAction::ShareAcknowledge { .. }
        | ScenarioAction::DropShareBatch { .. }
        | ScenarioAction::CloseShareConsumer { .. } => Some(Capability::ShareConsumer),
        ScenarioAction::CreateTopic(_)
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
        | ScenarioAction::DescribeClassicGroups(_) => Some(Capability::Admin),
        ScenarioAction::CreateTransactionalProducer { .. }
        | ScenarioAction::ExecuteTransaction { .. }
        | ScenarioAction::ExecuteTransactionalTransform(_)
        | ScenarioAction::FenceTransaction { .. }
        | ScenarioAction::CloseTransactionalProducer(_) => Some(Capability::Transactions),
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
    for &(capability, message) in REQUIRED_USAGE {
        require(
            usage.contains(&capability),
            capability,
            message,
            scenario,
            problems,
        );
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

const REQUIRED_USAGE: &[(Capability, &str)] = &[
    (
        Capability::ConcurrentActors,
        "concurrent actor steps require the concurrent_actors capability",
    ),
    (
        Capability::ProducerConfiguration,
        "configured-client steps require the producer_configuration capability",
    ),
    (
        Capability::ProducerCancellation,
        "cancel-send steps require the producer_cancellation capability",
    ),
    (
        Capability::ProducerBatch,
        "batch-send steps require the producer_batch capability",
    ),
    (
        Capability::AssignedConsumer,
        "assigned-consumer steps require the assigned_consumer capability",
    ),
    (
        Capability::AssignedConsumerControls,
        "assigned-consumer controls require the assigned_consumer_controls capability",
    ),
    (
        Capability::ConsumerGroups,
        "classic group-consumer steps require the consumer_groups capability",
    ),
    (
        Capability::ConsumerProtocolGroups,
        "KIP-848 group-consumer steps require the consumer_protocol_groups capability",
    ),
    (
        Capability::GroupConsumerControls,
        "group-consumer controls require the group_consumer_controls capability",
    ),
    (
        Capability::GroupConsumerConfiguration,
        "configured group consumers require the group_consumer_configuration capability",
    ),
    (
        Capability::GroupConsumerShutdown,
        "group shutdown steps require the group_consumer_shutdown capability",
    ),
    (
        Capability::ShareConsumer,
        "share-consumer steps require the share_consumer capability",
    ),
    (
        Capability::ShareConsumerConfiguration,
        "configured share consumers require the share_consumer_configuration capability",
    ),
    (
        Capability::Admin,
        "admin steps require the admin capability",
    ),
    (
        Capability::Transactions,
        "transaction steps require the transactions capability",
    ),
    (
        Capability::ModelBroker,
        "broker-control steps require the model_broker capability",
    ),
    (
        Capability::ClientReadiness,
        "client readiness steps require the client_readiness capability",
    ),
    (
        Capability::ClientMetrics,
        "client metrics steps require the client_metrics capability",
    ),
];

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
