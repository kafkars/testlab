//! Issued-action lookup keeps specialized command ownership ahead of generic lifecycle state.

use testlab_schema::ScenarioAction;

use super::HistoryIndex;

impl HistoryIndex {
    pub(crate) fn action_issued(&self, action: &ScenarioAction) -> bool {
        if let Some(issued) = self.admin_action_issued(action) {
            return issued;
        }
        if !self.has_harness_commands {
            return true;
        }
        if let Some(issued) = self.share_action_issued(action) {
            return issued;
        }
        if let Some(issued) = self.consumer_action_issued(action) {
            return issued;
        }
        if let Some(issued) = self.client_action_issued(action) {
            return issued;
        }
        if let Some(issued) = self.transaction_action_issued(action) {
            return issued;
        }
        self.generic_action_issued(action)
    }

    fn client_action_issued(&self, action: &ScenarioAction) -> Option<bool> {
        match action {
            ScenarioAction::CreateClient(testlab_schema::CreateClientAction {
                client_id, ..
            }) => Some(self.clients_create_issued.contains(client_id)),
            ScenarioAction::CreateConfiguredClient(action) => {
                Some(self.clients_create_issued.contains(&action.client_id))
            }
            ScenarioAction::CreateAssignedConsumerClient(action) => {
                Some(self.clients_create_issued.contains(&action.client_id))
            }
            ScenarioAction::AwaitClientReady { client_id } => {
                Some(self.clients_ready_issued.contains(client_id))
            }
            ScenarioAction::ObserveClientMetrics(action) => Some(
                self.client_metrics_issued.get(&action.operation_id) == Some(&action.client_id),
            ),
            _ => None,
        }
    }

    fn transaction_action_issued(&self, action: &ScenarioAction) -> Option<bool> {
        match action {
            ScenarioAction::CreateTransactionalProducer { producer_id, .. } => Some(
                self.transactional_producers_create_issued
                    .contains(producer_id),
            ),
            ScenarioAction::ExecuteTransaction { transaction_id, .. }
            | ScenarioAction::ExecuteTransactionalTransform(
                testlab_schema::TransactionalTransformAction { transaction_id, .. },
            )
            | ScenarioAction::FenceTransaction { transaction_id, .. } => {
                Some(self.transactions_execute_issued.contains(transaction_id))
            }
            ScenarioAction::CloseTransactionalProducer(action) => Some(
                self.transactional_producers_close_issued
                    .contains(&action.producer_id),
            ),
            _ => None,
        }
    }

    fn generic_action_issued(&self, action: &ScenarioAction) -> bool {
        match action {
            ScenarioAction::CreateClient(testlab_schema::CreateClientAction { .. })
            | ScenarioAction::CreateConfiguredClient(_)
            | ScenarioAction::CreateAssignedConsumerClient(_)
            | ScenarioAction::AwaitClientReady { .. }
            | ScenarioAction::ObserveClientMetrics(_) => {
                unreachable!("client actions are indexed before generic actions")
            }
            ScenarioAction::CreateProducer { producer_id, .. } => {
                self.producers_create_issued.contains(producer_id)
            }
            ScenarioAction::Send { operation_id, .. } => {
                self.operations_issued.contains(operation_id)
            }
            ScenarioAction::CancelProducerSend(action) => {
                self.operations_issued.contains(&action.operation_id)
            }
            ScenarioAction::SendBatch { operations, .. } => operations
                .iter()
                .all(|operation| self.operations_issued.contains(&operation.operation_id)),
            ScenarioAction::StartConcurrentActors(action) => {
                self.concurrent_starts.contains_key(&action.concurrency_id)
            }
            ScenarioAction::JoinConcurrentActors(action) => {
                self.concurrent_joins.contains_key(&action.concurrency_id)
            }
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
            | ScenarioAction::AlterConsumerGroupOffset(_)
            | ScenarioAction::DeleteConsumerGroupOffset(_)
            | ScenarioAction::DeleteConsumerGroup(_)
            | ScenarioAction::DeleteConsumerGroups(_)
            | ScenarioAction::RemoveConsumerGroupMembers(_)
            | ScenarioAction::ListConsumerGroupOffsetsBatch(_)
            | ScenarioAction::ListConsumerGroupsOffsets(_)
            | ScenarioAction::AlterConsumerGroupOffsets(_)
            | ScenarioAction::DeleteConsumerGroupOffsets(_)
            | ScenarioAction::DescribeClassicGroups(_)
            | ScenarioAction::DescribeConsumerGroups(_)
            | ScenarioAction::CreateAcls(_)
            | ScenarioAction::DescribeAcls(_)
            | ScenarioAction::DeleteAcls(_)
            | ScenarioAction::AlterClientQuota(_)
            | ScenarioAction::DescribeClientQuota(_)
            | ScenarioAction::AlterUserScramCredential(_)
            | ScenarioAction::DescribeUserScramCredential(_) => {
                unreachable!("admin actions are indexed before generic actions")
            }
            ScenarioAction::CreateTransactionalProducer { .. }
            | ScenarioAction::ExecuteTransaction { .. }
            | ScenarioAction::ExecuteTransactionalTransform(_)
            | ScenarioAction::FenceTransaction { .. }
            | ScenarioAction::CloseTransactionalProducer(_) => {
                unreachable!("transaction actions are indexed before generic actions")
            }
            ScenarioAction::Flush { producer_id } => self.flushes_issued.contains(producer_id),
            ScenarioAction::CloseProducer { producer_id } => {
                self.producers_close_issued.contains(producer_id)
            }
            ScenarioAction::ShutdownClient { client_id } => {
                self.clients_shutdown_issued.contains(client_id)
            }
            ScenarioAction::SetBrokerBehavior { .. }
            | ScenarioAction::ArmProtocolFault(_)
            | ScenarioAction::AlterNetworkFault(_)
            | ScenarioAction::CutNetworkConnections(_)
            | ScenarioAction::RestartBroker { .. }
            | ScenarioAction::StopBroker { .. }
            | ScenarioAction::StartBroker { .. }
            | ScenarioAction::StopBrokerRole { .. }
            | ScenarioAction::RestoreBrokerRole { .. }
            | ScenarioAction::AlterBrokerPolicy(_) => true,
            ScenarioAction::CreateShareConsumer { .. }
            | ScenarioAction::ShareReceive { .. }
            | ScenarioAction::ShareAcknowledge { .. }
            | ScenarioAction::DropShareBatch { .. }
            | ScenarioAction::CloseShareConsumer { .. } => {
                unreachable!("share actions are indexed before generic actions")
            }
            ScenarioAction::CreateAssignedConsumer { .. }
            | ScenarioAction::AssignBeginning { .. }
            | ScenarioAction::AssignBeginningBatch(_)
            | ScenarioAction::ControlAssignedConsumer(_)
            | ScenarioAction::ObserveAssignedConsumerEvent(_)
            | ScenarioAction::Receive { .. }
            | ScenarioAction::CloseAssignedConsumer { .. }
            | ScenarioAction::CreateGroupConsumer { .. }
            | ScenarioAction::GroupReceive { .. }
            | ScenarioAction::ObserveGroupAssignments(_)
            | ScenarioAction::GroupReceiveSet(_)
            | ScenarioAction::ControlGroupConsumer(_)
            | ScenarioAction::ShutdownGroupConsumer(_)
            | ScenarioAction::CloseGroupConsumer { .. }
            | ScenarioAction::AbandonGroupConsumer(_) => {
                unreachable!("consumer actions are indexed before generic actions")
            }
        }
    }

    fn consumer_action_issued(&self, action: &ScenarioAction) -> Option<bool> {
        match action {
            ScenarioAction::CreateAssignedConsumer { consumer_id, .. } => {
                Some(self.consumers_create_issued.contains(consumer_id))
            }
            ScenarioAction::AssignBeginning { consumer_id, .. } => {
                Some(self.assignments_issued.contains(consumer_id))
            }
            ScenarioAction::AssignBeginningBatch(action) => {
                Some(self.assignments_issued.contains(&action.consumer_id))
            }
            ScenarioAction::ControlAssignedConsumer(action) => Some(
                self.assigned_controls_issued
                    .contains_key(&action.operation_id),
            ),
            ScenarioAction::ObserveAssignedConsumerEvent(action) => {
                Some(self.commands.iter().any(|(_, _, command)| {
                    matches!(command,
                    testlab_schema::AdapterCommand::ObserveAssignedConsumerEvent(command)
                        if command.operation_id == action.operation_id)
                }))
            }
            ScenarioAction::Receive { receive_id, .. }
            | ScenarioAction::GroupReceive { receive_id, .. } => {
                Some(self.receives_issued.contains(receive_id))
            }
            ScenarioAction::CloseAssignedConsumer { consumer_id } => {
                Some(self.consumers_close_issued.contains(consumer_id))
            }
            ScenarioAction::CreateGroupConsumer { consumer_id, .. } => {
                Some(self.group_consumers_create_issued.contains(consumer_id))
            }
            ScenarioAction::ObserveGroupAssignments(action) => {
                Some(self.group_assignments_issued.contains(&action.operation_id))
            }
            ScenarioAction::GroupReceiveSet(action) => {
                Some(self.group_receive_sets_issued.contains(&action.receive_id))
            }
            ScenarioAction::ControlGroupConsumer(action) => Some(
                self.group_controls_issued
                    .contains_key(&action.operation_id),
            ),
            ScenarioAction::ShutdownGroupConsumer(action) => {
                Some(self.group_shutdowns_issued.contains(&action.operation_id))
            }
            ScenarioAction::CloseGroupConsumer { consumer_id } => {
                Some(self.group_consumers_close_issued.contains(consumer_id))
            }
            ScenarioAction::AbandonGroupConsumer(action) => Some(
                self.group_consumers_abandon_issued
                    .contains(&action.consumer_id),
            ),
            _ => None,
        }
    }

    pub(crate) fn finish_issued(&self) -> bool {
        !self.has_harness_commands || self.finish_issued
    }
}
