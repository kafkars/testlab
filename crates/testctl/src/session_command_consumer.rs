//! Consumer action translation owns assigned, group, and share command identities.

use testlab_schema::{AdapterCommand, ScenarioAction};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
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
            protocol,
        } => (
            AdapterCommand::CreateGroupConsumer {
                client_id: client_id.clone(),
                consumer_id: consumer_id.clone(),
                group_id: group_id.clone(),
                topic: topic.clone(),
                protocol: *protocol,
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
        action => return translate_share(action),
    };
    Some(pair)
}

fn translate_share(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    let pair = match action {
        ScenarioAction::CreateShareConsumer {
            client_id,
            consumer_id,
            group_id,
            topic,
            membership_timeout_ms,
            close_timeout_ms,
        } => (
            AdapterCommand::CreateShareConsumer {
                client_id: client_id.clone(),
                consumer_id: consumer_id.clone(),
                group_id: group_id.clone(),
                topic: topic.clone(),
                membership_timeout_ms: *membership_timeout_ms,
                close_timeout_ms: *close_timeout_ms,
            },
            ExpectedEvent::ShareConsumerCreated(consumer_id.clone()),
        ),
        ScenarioAction::ShareReceive {
            consumer_id,
            receive_id,
            timeout_ms,
            ..
        } => (
            AdapterCommand::ShareReceive {
                consumer_id: consumer_id.clone(),
                receive_id: receive_id.clone(),
                timeout_ms: *timeout_ms,
            },
            ExpectedEvent::ShareReceiveCompleted(receive_id.clone()),
        ),
        ScenarioAction::ShareAcknowledge {
            consumer_id,
            receive_id,
            acknowledgement_id,
            disposition,
            timeout_ms,
        } => (
            AdapterCommand::ShareAcknowledge {
                consumer_id: consumer_id.clone(),
                receive_id: receive_id.clone(),
                acknowledgement_id: acknowledgement_id.clone(),
                disposition: *disposition,
                timeout_ms: *timeout_ms,
            },
            ExpectedEvent::ShareAcknowledgementCompleted(acknowledgement_id.clone()),
        ),
        ScenarioAction::DropShareBatch {
            consumer_id,
            receive_id,
        } => (
            AdapterCommand::DropShareBatch {
                consumer_id: consumer_id.clone(),
                receive_id: receive_id.clone(),
            },
            ExpectedEvent::ShareBatchDropped(receive_id.clone()),
        ),
        ScenarioAction::CloseShareConsumer { consumer_id, .. } => (
            AdapterCommand::CloseShareConsumer {
                consumer_id: consumer_id.clone(),
            },
            ExpectedEvent::ShareConsumerClosed(consumer_id.clone()),
        ),
        _ => return None,
    };
    Some(pair)
}
