//! Scenario actions translate declaratively into correlated adapter expectations.

use testlab_schema::{AdapterCommand, ScenarioAction};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    let pair = match action {
        ScenarioAction::CreateClient { client_id } => (
            AdapterCommand::CreateClient {
                client_id: client_id.clone(),
            },
            ExpectedEvent::ClientCreated(client_id.clone()),
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
        | ScenarioAction::Receive { .. }
        | ScenarioAction::CloseAssignedConsumer { .. }
        | ScenarioAction::CreateGroupConsumer { .. }
        | ScenarioAction::GroupReceive { .. }
        | ScenarioAction::CloseGroupConsumer { .. }
        | ScenarioAction::CreateShareConsumer { .. }
        | ScenarioAction::ShareReceive { .. }
        | ScenarioAction::ShareAcknowledge { .. }
        | ScenarioAction::DropShareBatch { .. }
        | ScenarioAction::CloseShareConsumer { .. }) => {
            return crate::session_command_consumer::translate(action);
        }
        action @ ScenarioAction::CreateTopic { .. } => return admin(action),
        action @ (ScenarioAction::CreateTransactionalProducer { .. }
        | ScenarioAction::ExecuteTransaction { .. }
        | ScenarioAction::FenceTransaction { .. }
        | ScenarioAction::CloseTransactionalProducer { .. }) => return transaction(action),
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
        ScenarioAction::SetBrokerBehavior { .. }
        | ScenarioAction::RestartBroker { .. }
        | ScenarioAction::StopBroker { .. }
        | ScenarioAction::StartBroker { .. }
        | ScenarioAction::StopPartitionLeader { .. }
        | ScenarioAction::RestorePartitionLeader { .. } => {
            return None;
        }
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
        ScenarioAction::CloseTransactionalProducer { producer_id } => (
            AdapterCommand::CloseTransactionalProducer {
                producer_id: producer_id.clone(),
            },
            ExpectedEvent::TransactionalProducerClosed(producer_id.clone()),
        ),
        _ => return None,
    };
    Some(pair)
}

fn admin(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    let ScenarioAction::CreateTopic {
        client_id,
        operation_id,
        topic,
        partitions,
        replication_factor,
        timeout_ms,
    } = action
    else {
        return None;
    };
    Some((
        AdapterCommand::CreateTopic {
            client_id: client_id.clone(),
            operation_id: operation_id.clone(),
            topic: topic.clone(),
            partitions: *partitions,
            replication_factor: *replication_factor,
            timeout_ms: *timeout_ms,
        },
        ExpectedEvent::TopicCreated {
            operation_id: operation_id.clone(),
            topic: topic.clone(),
        },
    ))
}
