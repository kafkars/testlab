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
            configuration,
        } => (
            AdapterCommand::CreateGroupConsumer {
                client_id: client_id.clone(),
                consumer_id: consumer_id.clone(),
                group_id: group_id.clone(),
                topic: topic.clone(),
                protocol: *protocol,
                configuration: *configuration,
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
        ScenarioAction::ShutdownGroupConsumer(action) => {
            let completion = testlab_schema::GroupConsumerShutdownCompletion {
                operation_id: action.operation_id.clone(),
                consumer_id: action.consumer_id.clone(),
                request_count: action.request_count,
            };
            (
                AdapterCommand::ShutdownGroupConsumer(
                    testlab_schema::GroupConsumerShutdownCommand {
                        operation_id: action.operation_id.clone(),
                        consumer_id: action.consumer_id.clone(),
                        request_count: action.request_count,
                        timeout_ms: action.timeout_ms,
                    },
                ),
                ExpectedEvent::GroupConsumerShutdownCompleted(completion),
            )
        }
        action => return translate_ownership(action).or_else(|| translate_share(action)),
    };
    Some(pair)
}

fn translate_ownership(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    let pair = match action {
        ScenarioAction::AssignBeginningBatch(action) => (
            AdapterCommand::AssignBeginningBatch(testlab_schema::AssignBeginningBatchCommand {
                consumer_id: action.consumer_id.clone(),
                partitions: action.partitions.clone(),
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::AssignmentCompleted(action.consumer_id.clone()),
        ),
        ScenarioAction::ControlAssignedConsumer(action) => {
            let completion = testlab_schema::AssignedConsumerControlCompletion {
                operation_id: action.operation_id.clone(),
                consumer_id: action.consumer_id.clone(),
                control: action.control.kind(),
            };
            (
                AdapterCommand::ControlAssignedConsumer(
                    testlab_schema::AssignedConsumerControlCommand {
                        operation_id: action.operation_id.clone(),
                        consumer_id: action.consumer_id.clone(),
                        control: action.control.clone(),
                        timeout_ms: action.timeout_ms,
                    },
                ),
                ExpectedEvent::AssignedConsumerControlCompleted(completion),
            )
        }
        ScenarioAction::ObserveGroupAssignments(action) => (
            AdapterCommand::ObserveGroupAssignments(
                testlab_schema::ObserveGroupAssignmentsCommand {
                    operation_id: action.operation_id.clone(),
                    consumer_ids: action.consumer_ids.clone(),
                    timeout_ms: action.timeout_ms,
                },
            ),
            ExpectedEvent::GroupAssignmentsObserved(action.operation_id.clone()),
        ),
        ScenarioAction::GroupReceiveSet(action) => (
            AdapterCommand::GroupReceiveSet(testlab_schema::GroupReceiveSetCommand {
                receive_id: action.receive_id.clone(),
                consumer_ids: action.consumer_ids.clone(),
                record_count: action.expected_operation_ids.len(),
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::GroupReceiveSetCompleted(action.receive_id.clone()),
        ),
        ScenarioAction::ControlGroupConsumer(action) => {
            let completion = testlab_schema::GroupConsumerControlCompletion {
                operation_id: action.operation_id.clone(),
                consumer_id: action.consumer_id.clone(),
                control: action.control.kind(),
            };
            (
                AdapterCommand::ControlGroupConsumer(testlab_schema::GroupConsumerControlCommand {
                    operation_id: action.operation_id.clone(),
                    consumer_id: action.consumer_id.clone(),
                    control: action.control.clone(),
                    timeout_ms: action.timeout_ms,
                }),
                ExpectedEvent::GroupConsumerControlCompleted(completion),
            )
        }
        _ => return None,
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
            configuration,
        } => (
            AdapterCommand::CreateShareConsumer {
                client_id: client_id.clone(),
                consumer_id: consumer_id.clone(),
                group_id: group_id.clone(),
                topic: topic.clone(),
                membership_timeout_ms: *membership_timeout_ms,
                close_timeout_ms: *close_timeout_ms,
                configuration: *configuration,
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
            dispositions,
            timeout_ms,
        } => (
            AdapterCommand::ShareAcknowledge {
                consumer_id: consumer_id.clone(),
                receive_id: receive_id.clone(),
                acknowledgement_id: acknowledgement_id.clone(),
                dispositions: dispositions.clone(),
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
