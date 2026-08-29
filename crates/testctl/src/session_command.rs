//! Scenario actions translate declaratively into correlated adapter expectations.

use testlab_schema::{AdapterCommand, ScenarioAction};

use crate::runner_protocol::ExpectedEvent;

#[allow(
    clippy::too_many_lines,
    reason = "the exhaustive translator keeps every scenario action visibly routed"
)]
pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    let pair = match action {
        action @ (ScenarioAction::CreateClient { .. }
        | ScenarioAction::CreateConfiguredClient(_)
        | ScenarioAction::AwaitClientReady { .. }
        | ScenarioAction::CreateProducer { .. }) => return creation(action),
        ScenarioAction::ObserveClientMetrics(action) => (
            AdapterCommand::ObserveClientMetrics(testlab_schema::ObserveClientMetricsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
            }),
            ExpectedEvent::ClientMetricsObserved(
                action.client_id.clone(),
                action.operation_id.clone(),
            ),
        ),
        ScenarioAction::Send {
            producer_id,
            operation_id,
            record,
        } => (
            AdapterCommand::Send {
                producer_id: producer_id.clone(),
                operation_id: operation_id.clone(),
                record: record.clone(),
            },
            ExpectedEvent::SendSettled(operation_id.clone()),
        ),
        ScenarioAction::CancelProducerSend(action) => (
            AdapterCommand::CancelProducerSend(action.clone()),
            ExpectedEvent::ProducerCancellationCompleted(action.operation_id.clone()),
        ),
        ScenarioAction::SendBatch {
            producer_id,
            operations,
        } => (
            AdapterCommand::SendBatch {
                producer_id: producer_id.clone(),
                operations: operations.clone(),
            },
            ExpectedEvent::BatchCompleted {
                producer_id: producer_id.clone(),
                operation_ids: operations
                    .iter()
                    .map(|operation| operation.operation_id.clone())
                    .collect(),
            },
        ),
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
        | ScenarioAction::CloseGroupConsumer { .. }
        | ScenarioAction::CreateShareConsumer { .. }
        | ScenarioAction::ShareReceive { .. }
        | ScenarioAction::ShareAcknowledge { .. }
        | ScenarioAction::DropShareBatch { .. }
        | ScenarioAction::CloseShareConsumer { .. }) => {
            return crate::session_command_consumer::translate(action);
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
            return crate::session_command_admin::translate(action);
        }
        action @ (ScenarioAction::CreateTransactionalProducer { .. }
        | ScenarioAction::ExecuteTransaction { .. }
        | ScenarioAction::ExecuteTransactionalTransform(_)
        | ScenarioAction::FenceTransaction { .. }
        | ScenarioAction::CloseTransactionalProducer(_)) => return transaction(action),
        ScenarioAction::Flush { producer_id } => (
            AdapterCommand::Flush {
                producer_id: producer_id.clone(),
            },
            ExpectedEvent::FlushCompleted(producer_id.clone()),
        ),
        ScenarioAction::CloseProducer { producer_id } => (
            AdapterCommand::CloseProducer {
                producer_id: producer_id.clone(),
            },
            ExpectedEvent::ProducerClosed(producer_id.clone()),
        ),
        ScenarioAction::ShutdownClient { client_id } => (
            AdapterCommand::ShutdownClient {
                client_id: client_id.clone(),
            },
            ExpectedEvent::ClientShutdown(client_id.clone()),
        ),
        ScenarioAction::StartConcurrentActors(_)
        | ScenarioAction::JoinConcurrentActors(_)
        | ScenarioAction::SetBrokerBehavior { .. }
        | ScenarioAction::ArmProtocolFault(_)
        | ScenarioAction::AlterNetworkFault(_)
        | ScenarioAction::CutNetworkConnections(_)
        | ScenarioAction::RestartBroker { .. }
        | ScenarioAction::StopBroker { .. }
        | ScenarioAction::StartBroker { .. }
        | ScenarioAction::StopBrokerRole { .. }
        | ScenarioAction::RestoreBrokerRole { .. }
        | ScenarioAction::AlterBrokerPolicy(_) => {
            return None;
        }
    };
    Some(pair)
}

fn creation(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    let pair = match action {
        ScenarioAction::CreateClient { client_id } => (
            AdapterCommand::CreateClient {
                client_id: client_id.clone(),
            },
            ExpectedEvent::ClientCreated(client_id.clone()),
        ),
        ScenarioAction::CreateConfiguredClient(action) => (
            AdapterCommand::CreateConfiguredClient(action.clone()),
            ExpectedEvent::ClientCreated(action.client_id.clone()),
        ),
        ScenarioAction::AwaitClientReady { client_id } => (
            AdapterCommand::AwaitClientReady {
                client_id: client_id.clone(),
            },
            ExpectedEvent::ClientReady(client_id.clone()),
        ),
        ScenarioAction::CreateProducer {
            client_id,
            producer_id,
        } => (
            AdapterCommand::CreateProducer {
                client_id: client_id.clone(),
                producer_id: producer_id.clone(),
            },
            ExpectedEvent::ProducerCreated(producer_id.clone()),
        ),
        _ => return None,
    };
    Some(pair)
}

fn transaction(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    let pair = match action {
        ScenarioAction::CreateTransactionalProducer {
            client_id,
            producer_id,
            transactional_id,
            transaction_timeout_ms,
            initialization_timeout_ms,
            ..
        } => (
            AdapterCommand::CreateTransactionalProducer {
                client_id: client_id.clone(),
                producer_id: producer_id.clone(),
                transactional_id: transactional_id.clone(),
                transaction_timeout_ms: *transaction_timeout_ms,
                initialization_timeout_ms: *initialization_timeout_ms,
            },
            ExpectedEvent::TransactionalProducerCreated(producer_id.clone()),
        ),
        ScenarioAction::ExecuteTransaction {
            producer_id,
            transaction_id,
            operations,
            disposition,
            timeout_ms,
        } => (
            AdapterCommand::ExecuteTransaction {
                producer_id: producer_id.clone(),
                transaction_id: transaction_id.clone(),
                operations: operations.clone(),
                disposition: *disposition,
                timeout_ms: *timeout_ms,
            },
            ExpectedEvent::TransactionCompleted {
                transaction_id: transaction_id.clone(),
                operation_ids: operations
                    .iter()
                    .map(|operation| operation.operation_id.clone())
                    .collect(),
            },
        ),
        ScenarioAction::ExecuteTransactionalTransform(action) => (
            AdapterCommand::ExecuteTransactionalTransform(
                testlab_schema::TransactionalTransformCommand {
                    producer_id: action.producer_id.clone(),
                    consumer_id: action.consumer_id.clone(),
                    transaction_id: action.transaction_id.clone(),
                    operations: action.operations.clone(),
                    disposition: action.disposition,
                    timeout_ms: action.timeout_ms,
                },
            ),
            ExpectedEvent::TransactionCompleted {
                transaction_id: action.transaction_id.clone(),
                operation_ids: action
                    .operations
                    .iter()
                    .map(|operation| operation.operation_id.clone())
                    .collect(),
            },
        ),
        ScenarioAction::FenceTransaction {
            producer_id,
            transaction_id,
            operation,
            replacement_client_id,
            replacement_producer_id,
            transactional_id,
            transaction_timeout_ms,
            initialization_timeout_ms,
            timeout_ms,
        } => (
            AdapterCommand::FenceTransaction {
                producer_id: producer_id.clone(),
                transaction_id: transaction_id.clone(),
                operation: operation.clone(),
                replacement_client_id: replacement_client_id.clone(),
                replacement_producer_id: replacement_producer_id.clone(),
                transactional_id: transactional_id.clone(),
                transaction_timeout_ms: *transaction_timeout_ms,
                initialization_timeout_ms: *initialization_timeout_ms,
                timeout_ms: *timeout_ms,
            },
            ExpectedEvent::TransactionFenceCompleted {
                transaction_id: transaction_id.clone(),
                operation_id: operation.operation_id.clone(),
                replacement_producer_id: replacement_producer_id.clone(),
            },
        ),
        ScenarioAction::CloseTransactionalProducer(action) => (
            AdapterCommand::CloseTransactionalProducer {
                producer_id: action.producer_id.clone(),
            },
            ExpectedEvent::TransactionalProducerClosed(action.producer_id.clone()),
        ),
        _ => return None,
    };
    Some(pair)
}
