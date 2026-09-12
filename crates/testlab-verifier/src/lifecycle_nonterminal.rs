//! Nonterminal actions settle through operation-specific contracts instead of lifecycle events.

use testlab_schema::ScenarioAction;

pub(crate) fn has_no_lifecycle_terminal(action: &ScenarioAction) -> bool {
    if let ScenarioAction::CreateClient(action) = action
        && action.expected_error_code.is_some()
    {
        return true;
    }
    matches!(
        action,
        ScenarioAction::SetBrokerBehavior { .. }
            | ScenarioAction::ArmProtocolFault(_)
            | ScenarioAction::RestartBroker { .. }
            | ScenarioAction::StopBroker { .. }
            | ScenarioAction::StartBroker { .. }
            | ScenarioAction::StopBrokerRole { .. }
            | ScenarioAction::RestoreBrokerRole { .. }
            | ScenarioAction::AlterBrokerPolicy(_)
            | ScenarioAction::FenceProducers(_)
            | ScenarioAction::AlterPartitionReassignments(_)
            | ScenarioAction::ListPartitionReassignments(_)
            | ScenarioAction::ElectLeaders(_)
            | ScenarioAction::Send { .. }
            | ScenarioAction::SendBatch { .. }
            | ScenarioAction::StartConcurrentActors(_)
            | ScenarioAction::JoinConcurrentActors(_)
            | ScenarioAction::Receive { .. }
            | ScenarioAction::TransferAssignedRecord(_)
            | ScenarioAction::ObserveAssignedConsumerEvent(_)
            | ScenarioAction::GroupReceive { .. }
            | ScenarioAction::ObserveGroupAssignments(_)
            | ScenarioAction::GroupReceiveSet(_)
            | ScenarioAction::AbandonGroupConsumer(_)
            | ScenarioAction::CreateTopic(_)
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
            | ScenarioAction::ListConsumerGroups(_)
            | ScenarioAction::DescribeConsumerGroup(_)
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
            | ScenarioAction::DescribeConsumerGroups(_)
            | ScenarioAction::CreateAcls(_)
            | ScenarioAction::DescribeAcls(_)
            | ScenarioAction::DeleteAcls(_)
            | ScenarioAction::AlterClientQuota(_)
            | ScenarioAction::DescribeClientQuota(_)
            | ScenarioAction::AlterUserScramCredential(_)
            | ScenarioAction::DescribeUserScramCredential(_)
            | ScenarioAction::DescribeTopicConfig(_)
            | ScenarioAction::DescribeTopicConfigs(_)
            | ScenarioAction::AlterTopicConfigs(_)
            | ScenarioAction::AlterTopicConfig(_)
            | ScenarioAction::ExecuteTransaction { .. }
            | ScenarioAction::FenceTransaction { .. }
            | ScenarioAction::CreateShareConsumer { .. }
            | ScenarioAction::ShareReceive { .. }
            | ScenarioAction::ShareAcknowledge { .. }
            | ScenarioAction::DropShareBatch { .. }
            | ScenarioAction::CloseShareConsumer { .. }
    )
}
