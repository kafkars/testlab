//! Capability validation binds used scenario actions to declared adapter support.

use std::collections::BTreeSet;

use crate::{Capability, ChildHandleOwnership, GroupProtocol, ScenarioAction};

#[path = "scenario_capability_required.rs"]
mod required;
pub(crate) use required::validate_required;

pub(crate) fn record_usage(action: &ScenarioAction, usage: &mut BTreeSet<Capability>) {
    if matches!(
        action,
        ScenarioAction::FenceTransaction {
            fence_method: crate::TransactionFenceMethod::AdminForceTermination,
            ..
        } | ScenarioAction::ExecuteTransaction {
            disposition: crate::TransactionDisposition::AdminPartitionAbort,
            ..
        }
    ) {
        usage.insert(Capability::Admin);
    }
    if matches!(
        action,
        ScenarioAction::CreateProducer {
            ownership: ChildHandleOwnership::Independent,
            ..
        } | ScenarioAction::CreateAssignedConsumer {
            ownership: ChildHandleOwnership::Independent,
            ..
        }
    ) {
        usage.insert(Capability::IndependentHandles);
    }
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
    if matches!(
        action,
        ScenarioAction::Send {
            method: crate::ProducerSendMethod::Send,
            ..
        } | ScenarioAction::CancelProducerSend(crate::CancelProducerSendCommand {
            method: crate::ProducerSendMethod::Send,
            ..
        })
    ) {
        usage.insert(Capability::ProducerWaitingSend);
    }
    if matches!(
        action,
        ScenarioAction::Receive {
            method: crate::AssignedConsumerReceiveMethod::TryTakeBatch,
            ..
        }
    ) {
        usage.insert(Capability::AssignedConsumerImmediateBatch);
    }
    let capability = match action {
        ScenarioAction::SetBrokerBehavior { .. } => Some(Capability::ModelBroker),
        ScenarioAction::CreateClient(action) if action.expected_cluster_id.is_some() => {
            Some(Capability::ExpectedClusterIdentity)
        }
        ScenarioAction::AwaitClientReady { .. } => Some(Capability::ClientReadiness),
        ScenarioAction::ObserveClientMetrics(_) => Some(Capability::ClientMetrics),
        ScenarioAction::CreateConfiguredClient(_) => Some(Capability::ProducerConfiguration),
        ScenarioAction::CreateAssignedConsumerClient(_) => {
            Some(Capability::AssignedConsumerConfiguration)
        }
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
        | ScenarioAction::DeleteTopics(_)
        | ScenarioAction::DescribeTopic(_)
        | ScenarioAction::DescribeTopics(_)
        | ScenarioAction::ListTopics(_)
        | ScenarioAction::ListConfigResources(_)
        | ScenarioAction::ListOffsets(_)
        | ScenarioAction::ListOffsetsBatch(_)
        | ScenarioAction::DeleteRecords(_)
        | ScenarioAction::DeleteRecordsBatch(_)
        | ScenarioAction::DescribeTopicConfig(_)
        | ScenarioAction::DescribeTopicConfigs(_)
        | ScenarioAction::AlterTopicConfigs(_)
        | ScenarioAction::AlterTopicConfig(_)
        | ScenarioAction::DescribeCluster(_)
        | ScenarioAction::UnregisterBroker(_)
        | ScenarioAction::DescribeFeatures(_)
        | ScenarioAction::ValidateFeatureUpdates(_)
        | ScenarioAction::ExerciseDelegationTokenLifecycle(_)
        | ScenarioAction::ExerciseStreamsGroupAdminLifecycle(_)
        | ScenarioAction::DescribeProducers(_)
        | ScenarioAction::DescribeLogDirs(_)
        | ScenarioAction::DescribeReplicaLogDirs(_)
        | ScenarioAction::AlterReplicaLogDirs(_)
        | ScenarioAction::DescribeMetadataQuorum(_)
        | ScenarioAction::ListTransactions(_)
        | ScenarioAction::DescribeTransactions(_)
        | ScenarioAction::FenceProducers(_)
        | ScenarioAction::AlterPartitionReassignments(_)
        | ScenarioAction::ListPartitionReassignments(_)
        | ScenarioAction::ElectLeaders(_)
        | ScenarioAction::ListConsumerGroups(_)
        | ScenarioAction::DescribeConsumerGroup(_)
        | ScenarioAction::DescribeConsumerGroups(_)
        | ScenarioAction::DescribeShareGroup(_)
        | ScenarioAction::DescribeShareGroups(_)
        | ScenarioAction::ListShareGroupOffsets(_)
        | ScenarioAction::ListShareGroupsOffsets(_)
        | ScenarioAction::AlterShareGroupOffsets(_)
        | ScenarioAction::DeleteShareGroupOffsets(_)
        | ScenarioAction::DeleteShareGroups(_)
        | ScenarioAction::ListConsumerGroupOffsets(_)
        | ScenarioAction::ListConsumerGroupOffsetsBatch(_)
        | ScenarioAction::ListConsumerGroupsOffsets(_)
        | ScenarioAction::AlterConsumerGroupOffset(_)
        | ScenarioAction::AlterConsumerGroupOffsets(_)
        | ScenarioAction::DeleteConsumerGroupOffset(_)
        | ScenarioAction::DeleteConsumerGroupOffsets(_)
        | ScenarioAction::DeleteConsumerGroup(_)
        | ScenarioAction::DeleteConsumerGroups(_)
        | ScenarioAction::RemoveConsumerGroupMembers(_)
        | ScenarioAction::DescribeClassicGroups(_)
        | ScenarioAction::CreateAcls(_)
        | ScenarioAction::DescribeAcls(_)
        | ScenarioAction::DeleteAcls(_)
        | ScenarioAction::AlterClientQuota(_)
        | ScenarioAction::DescribeClientQuota(_)
        | ScenarioAction::AlterUserScramCredential(_)
        | ScenarioAction::DescribeUserScramCredential(_) => Some(Capability::Admin),
        ScenarioAction::CreateTransactionalProducer { .. }
        | ScenarioAction::ExecuteTransaction { .. }
        | ScenarioAction::ExecuteTransactionalTransform(_)
        | ScenarioAction::FenceTransaction { .. }
        | ScenarioAction::CloseTransactionalProducer(_) => Some(Capability::Transactions),
        _ => None,
    };
    usage.extend(capability);
}
