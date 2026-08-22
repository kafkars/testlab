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
        | ScenarioAction::CloseGroupConsumer { .. }) => return consumer(action),
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
        ScenarioAction::SetBrokerBehavior { .. } => return None,
    };
    Some(pair)
}

fn consumer(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    let pair = match action {
        ScenarioAction::CreateAssignedConsumer {
            client_id,
            consumer_id,
        } => (
            AdapterCommand::CreateAssignedConsumer {
                client_id: client_id.clone(),
                consumer_id: consumer_id.clone(),
            },
            ExpectedEvent::AssignedConsumerCreated(consumer_id.clone()),
        ),
        ScenarioAction::AssignBeginning {
            consumer_id,
            topic,
            partition,
        } => (
            AdapterCommand::AssignBeginning {
                consumer_id: consumer_id.clone(),
                topic: topic.clone(),
                partition: *partition,
            },
            ExpectedEvent::AssignmentCompleted(consumer_id.clone()),
        ),
        ScenarioAction::Receive {
            consumer_id,
            receive_id,
            timeout_ms,
            ..
        } => (
            AdapterCommand::Receive {
                consumer_id: consumer_id.clone(),
                receive_id: receive_id.clone(),
                timeout_ms: *timeout_ms,
            },
            ExpectedEvent::ReceiveCompleted(receive_id.clone()),
        ),
        ScenarioAction::CloseAssignedConsumer { consumer_id } => (
            AdapterCommand::CloseAssignedConsumer {
                consumer_id: consumer_id.clone(),
            },
            ExpectedEvent::AssignedConsumerClosed(consumer_id.clone()),
        ),
        ScenarioAction::CreateGroupConsumer {
            client_id,
            consumer_id,
            group_id,
            topic,
        } => (
            AdapterCommand::CreateGroupConsumer {
                client_id: client_id.clone(),
                consumer_id: consumer_id.clone(),
                group_id: group_id.clone(),
                topic: topic.clone(),
            },
            ExpectedEvent::GroupConsumerCreated(consumer_id.clone()),
        ),
        ScenarioAction::GroupReceive {
            consumer_id,
            receive_id,
            timeout_ms,
            ..
        } => (
            AdapterCommand::GroupReceive {
                consumer_id: consumer_id.clone(),
                receive_id: receive_id.clone(),
                timeout_ms: *timeout_ms,
            },
            ExpectedEvent::GroupReceiveCompleted(receive_id.clone()),
        ),
        ScenarioAction::CloseGroupConsumer { consumer_id } => (
            AdapterCommand::CloseGroupConsumer {
                consumer_id: consumer_id.clone(),
            },
            ExpectedEvent::GroupConsumerClosed(consumer_id.clone()),
        ),
        _ => return None,
    };
    Some(pair)
}
