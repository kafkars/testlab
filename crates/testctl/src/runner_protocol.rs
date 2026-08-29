//! Expected event shapes constrain each sequential protocol-v34 command.

use std::collections::BTreeSet;

use testlab_schema::{AdapterEvent, ClientId, ConsumerId, OperationId, ProducerId};

use crate::run_error::RunFailure;
use crate::runner_protocol_admin::classify_admin;
use crate::runner_protocol_admin_config::classify as classify_admin_config;
use crate::runner_protocol_admin_group_batch::classify as classify_admin_group_batch;
use crate::runner_protocol_family::{classify_group, classify_transaction, same_event_family};

pub(crate) use crate::runner_protocol_event::EventDisposition;

#[derive(Clone, Debug)]
pub(crate) enum ExpectedEvent {
    Ready,
    ClientCreated(ClientId),
    ClientReady(ClientId),
    ClientMetricsObserved(ClientId, OperationId),
    ProducerCreated(ProducerId),
    SendSettled(OperationId),
    ProducerCancellationCompleted(OperationId),
    BatchCompleted {
        producer_id: ProducerId,
        operation_ids: BTreeSet<OperationId>,
    },
    ConcurrentActorsStarted(crate::runner_protocol_concurrent::ConcurrentExpectation),
    ConcurrentActorsCompleted(crate::runner_protocol_concurrent::ConcurrentExpectation),
    AssignedConsumerCreated(ConsumerId),
    AssignmentCompleted(ConsumerId),
    AssignedConsumerControlCompleted(testlab_schema::AssignedConsumerControlCompletion),
    ReceiveCompleted(OperationId),
    AssignedConsumerClosed(ConsumerId),
    GroupConsumerCreated(ConsumerId),
    GroupReceiveCompleted(OperationId),
    GroupAssignmentsObserved(OperationId),
    GroupReceiveSetCompleted(OperationId),
    GroupConsumerControlCompleted(testlab_schema::GroupConsumerControlCompletion),
    GroupConsumerShutdownCompleted(testlab_schema::GroupConsumerShutdownCompletion),
    GroupConsumerClosed(ConsumerId),
    ShareConsumerCreated(ConsumerId),
    ShareReceiveCompleted(OperationId),
    ShareAcknowledgementCompleted(OperationId),
    ShareBatchDropped(OperationId),
    ShareConsumerClosed(ConsumerId),
    TopicCreated {
        operation_id: OperationId,
        topic: String,
    },
    TopicCreationValidated {
        operation_id: OperationId,
        topic: String,
    },
    TopicsCreationCompleted {
        operation_id: OperationId,
    },
    TopicPartitionsCreated {
        operation_id: OperationId,
        topic: String,
    },
    TopicPartitionIncreaseValidated {
        operation_id: OperationId,
        topic: String,
    },
    TopicDeleted {
        operation_id: OperationId,
        topic: String,
    },
    TopicDescribed {
        operation_id: OperationId,
        topic: String,
    },
    TopicsListed {
        operation_id: OperationId,
    },
    OffsetListed {
        operation_id: OperationId,
        topic: String,
        partition: i32,
    },
    RecordsDeleted {
        operation_id: OperationId,
        topic: String,
        partition: i32,
    },
    TopicConfigDescribed {
        operation_id: OperationId,
        topic: String,
        config_name: String,
    },
    TopicConfigAltered {
        operation_id: OperationId,
        topic: String,
        config_name: String,
    },
    TopicConfigAlterationValidated {
        operation_id: OperationId,
        topic: String,
        config_name: String,
    },
    ClusterDescribed {
        operation_id: OperationId,
    },
    ConsumerGroupsListed {
        operation_id: OperationId,
    },
    ConsumerGroupDescribed {
        operation_id: OperationId,
        group_id: String,
    },
    ConsumerGroupOffsetListed {
        operation_id: OperationId,
        group_id: String,
        topic: String,
        partition: i32,
    },
    ConsumerGroupOffsetAltered {
        operation_id: OperationId,
        group_id: String,
        topic: String,
        partition: i32,
    },
    ConsumerGroupOffsetDeleted {
        operation_id: OperationId,
        group_id: String,
        topic: String,
        partition: i32,
    },
    ConsumerGroupDeleted {
        operation_id: OperationId,
        group_id: String,
    },
    ConsumerGroupOffsetsListed {
        operation_id: OperationId,
    },
    ConsumerGroupsOffsetsListed {
        operation_id: OperationId,
    },
    ConsumerGroupOffsetsAltered {
        operation_id: OperationId,
    },
    ConsumerGroupOffsetsDeleted {
        operation_id: OperationId,
    },
    ClassicGroupsDescribed {
        operation_id: OperationId,
    },
    TransactionalProducerCreated(ProducerId),
    TransactionCompleted {
        transaction_id: OperationId,
        operation_ids: BTreeSet<OperationId>,
    },
    TransactionFenceCompleted {
        transaction_id: OperationId,
        operation_id: OperationId,
        replacement_producer_id: ProducerId,
    },
    TransactionalProducerClosed(ProducerId),
    FlushCompleted(ProducerId),
    ProducerClosed(ProducerId),
    ClientShutdown(ClientId),
    Finished,
    Aborted,
}

impl ExpectedEvent {
    pub(crate) fn classify(&self, event: &AdapterEvent) -> Result<EventDisposition, RunFailure> {
        if matches!(event, AdapterEvent::CommandFailed { .. }) {
            return Ok(EventDisposition::Complete);
        }
        if let Some(disposition) = crate::runner_protocol_concurrent::classify(self, event) {
            return disposition;
        }
        if let Some(disposition) = classify_group(self, event) {
            return disposition;
        }
        if let Some(disposition) = crate::runner_protocol_share::classify(self, event) {
            return disposition;
        }
        if let Some(disposition) = classify_admin_config(self, event) {
            return disposition;
        }
        if let Some(disposition) = classify_admin_group_batch(self, event) {
            return disposition;
        }
        if let Some(disposition) = classify_admin(self, event) {
            return disposition;
        }
        if let Some(disposition) = crate::runner_protocol_cancel::classify(self, event) {
            return disposition;
        }
        if let Some(disposition) = classify_transaction(self, event) {
            return disposition;
        }
        classify_core(self, event)
    }
}

fn classify_core(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Result<EventDisposition, RunFailure> {
    match (expected, event) {
        (ExpectedEvent::Ready, AdapterEvent::Ready { .. })
        | (ExpectedEvent::Finished, AdapterEvent::Finished)
        | (ExpectedEvent::Aborted, AdapterEvent::Aborted) => Ok(EventDisposition::Complete),
        (ExpectedEvent::ClientCreated(expected), AdapterEvent::ClientCreated { client_id })
            if expected == client_id =>
        {
            Ok(EventDisposition::Complete)
        }
        (ExpectedEvent::ClientReady(expected), AdapterEvent::ClientReady { client_id })
            if expected == client_id =>
        {
            Ok(EventDisposition::Complete)
        }
        (
            ExpectedEvent::ClientMetricsObserved(client_id, operation_id),
            AdapterEvent::ClientMetricsObserved(observation),
        ) if client_id == &observation.client_id && operation_id == &observation.operation_id => {
            Ok(EventDisposition::Complete)
        }
        (
            ExpectedEvent::ProducerCreated(expected),
            AdapterEvent::ProducerCreated { producer_id },
        ) if expected == producer_id => Ok(EventDisposition::Complete),
        (ExpectedEvent::FlushCompleted(expected), AdapterEvent::FlushCompleted { producer_id })
            if expected == producer_id =>
        {
            Ok(EventDisposition::Complete)
        }
        (ExpectedEvent::ProducerClosed(expected), AdapterEvent::ProducerClosed { producer_id })
            if expected == producer_id =>
        {
            Ok(EventDisposition::Complete)
        }
        (ExpectedEvent::ClientShutdown(expected), AdapterEvent::ClientShutdown { client_id })
            if expected == client_id =>
        {
            Ok(EventDisposition::Complete)
        }
        (
            ExpectedEvent::SendSettled(expected),
            AdapterEvent::OperationAccepted { operation_id },
        ) if expected == operation_id => Ok(EventDisposition::Continue),
        (
            ExpectedEvent::SendSettled(expected),
            AdapterEvent::OperationRejected { operation_id, .. }
            | AdapterEvent::OperationTerminal { operation_id, .. },
        ) if expected == operation_id => Ok(EventDisposition::Complete),
        (
            ExpectedEvent::BatchCompleted { operation_ids, .. },
            AdapterEvent::OperationAccepted { operation_id }
            | AdapterEvent::OperationRejected { operation_id, .. }
            | AdapterEvent::OperationTerminal { operation_id, .. },
        ) if operation_ids.contains(operation_id) => Ok(EventDisposition::Continue),
        (
            ExpectedEvent::BatchCompleted { producer_id, .. },
            AdapterEvent::BatchCompleted {
                producer_id: actual,
            },
        ) if producer_id == actual => Ok(EventDisposition::Complete),
        (
            ExpectedEvent::AssignedConsumerCreated(expected),
            AdapterEvent::AssignedConsumerCreated {
                consumer_id: actual,
            },
        ) if expected == actual => Ok(EventDisposition::Complete),
        (
            ExpectedEvent::AssignmentCompleted(expected),
            AdapterEvent::AssignmentCompleted {
                consumer_id: actual,
            },
        ) if expected == actual => Ok(EventDisposition::Complete),
        (
            ExpectedEvent::ReceiveCompleted(expected),
            AdapterEvent::ReceiveCompleted {
                receive_id: actual, ..
            },
        ) if expected == actual => Ok(EventDisposition::Complete),
        (
            ExpectedEvent::AssignedConsumerClosed(expected),
            AdapterEvent::AssignedConsumerClosed {
                consumer_id: actual,
            },
        ) if expected == actual => Ok(EventDisposition::Complete),
        _ if same_event_family(expected, event) => Err(RunFailure::protocol(
            "event_identity_mismatch",
            format!("event {event:?} does not match expected {expected:?}"),
        )),
        _ => Err(RunFailure::protocol(
            "unexpected_event_kind",
            format!("unexpected event {event:?} while waiting for {expected:?}"),
        )),
    }
}
