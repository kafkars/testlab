//! Event-family classification separates identity checks from session flow.

use testlab_schema::AdapterEvent;

use crate::run_error::RunFailure;
use crate::runner_protocol::{EventDisposition, ExpectedEvent};
use crate::runner_protocol_admin::same_admin_event_family;
use crate::runner_protocol_identity::{identity_mismatch, identity_result};

pub(super) fn classify_group(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    let identity_matches = match (expected, event) {
        (
            ExpectedEvent::AssignedConsumerControlCompleted(expected),
            AdapterEvent::AssignedConsumerControlCompleted(actual),
        ) => expected == actual,
        (
            ExpectedEvent::AssignedConsumerEventObserved(expected),
            AdapterEvent::AssignedConsumerEventObserved(actual),
        ) => expected == &actual.operation_id,
        (
            ExpectedEvent::GroupConsumerCreated(expected),
            AdapterEvent::GroupConsumerCreated {
                consumer_id: actual,
            },
        )
        | (
            ExpectedEvent::GroupConsumerClosed(expected),
            AdapterEvent::GroupConsumerClosed {
                consumer_id: actual,
            },
        ) => expected == actual,
        (
            ExpectedEvent::GroupConsumerAbandoned(expected),
            AdapterEvent::GroupConsumerAbandoned(actual),
        ) => expected == &actual.consumer_id,
        (
            ExpectedEvent::GroupReceiveCompleted(expected),
            AdapterEvent::GroupReceiveCompleted {
                receive_id: actual, ..
            },
        ) => expected == actual,
        (
            ExpectedEvent::GroupAssignmentsObserved(expected),
            AdapterEvent::GroupAssignmentsObserved(actual),
        ) => expected == &actual.operation_id,
        (
            ExpectedEvent::GroupReceiveSetCompleted(expected),
            AdapterEvent::GroupReceiveSetCompleted(actual),
        ) => expected == &actual.receive_id,
        (
            ExpectedEvent::GroupConsumerControlCompleted(expected),
            AdapterEvent::GroupConsumerControlCompleted(actual),
        ) => expected == actual,
        (
            ExpectedEvent::GroupConsumerShutdownCompleted(expected),
            AdapterEvent::GroupConsumerShutdownCompleted(actual),
        ) => expected == actual,
        _ => return None,
    };
    Some(if identity_matches {
        Ok(EventDisposition::Complete)
    } else {
        Err(identity_mismatch(event, expected))
    })
}

pub(super) fn same_event_family(expected: &ExpectedEvent, event: &AdapterEvent) -> bool {
    crate::runner_protocol_concurrent::same_event_family(expected, event)
        || crate::runner_protocol_cancel::same_event_family(expected, event)
        || same_admin_event_family(expected, event)
        || crate::runner_protocol_admin_acl::same_event_family(expected, event)
        || crate::runner_protocol_admin_client_quota::same_event_family(expected, event)
        || crate::runner_protocol_admin_user_scram::same_event_family(expected, event)
        || crate::runner_protocol_admin_share_group::same_event_family(expected, event)
        || crate::runner_protocol_admin_group_batch::same_event_family(expected, event)
        || (crate::runner_protocol_admin_config::expected(expected)
            && crate::runner_protocol_admin_config::event(event))
        || same_base_event_family(expected, event)
        || same_extended_event_family(expected, event)
}

fn same_base_event_family(expected: &ExpectedEvent, event: &AdapterEvent) -> bool {
    matches!(
        (expected, event),
        (ExpectedEvent::Ready, AdapterEvent::Ready { .. })
            | (
                ExpectedEvent::ClientReady(_),
                AdapterEvent::ClientReady { .. }
            )
            | (
                ExpectedEvent::ClientCreated(_),
                AdapterEvent::ClientCreated(..)
            )
            | (
                ExpectedEvent::ClientMetricsObserved(..),
                AdapterEvent::ClientMetricsObserved(_)
            )
            | (
                ExpectedEvent::ProducerCreated(_),
                AdapterEvent::ProducerCreated { .. }
            )
            | (
                ExpectedEvent::SendSettled(_),
                AdapterEvent::OperationAccepted { .. }
                    | AdapterEvent::OperationRejected { .. }
                    | AdapterEvent::OperationTerminal { .. }
            )
            | (
                ExpectedEvent::BatchCompleted { .. },
                AdapterEvent::OperationAccepted { .. }
                    | AdapterEvent::OperationRejected { .. }
                    | AdapterEvent::OperationTerminal { .. }
                    | AdapterEvent::BatchCompleted { .. }
            )
            | (
                ExpectedEvent::AssignedConsumerCreated(_),
                AdapterEvent::AssignedConsumerCreated { .. }
            )
            | (
                ExpectedEvent::AssignmentCompleted(_),
                AdapterEvent::AssignmentCompleted { .. }
            )
            | (
                ExpectedEvent::AssignedConsumerControlCompleted(_),
                AdapterEvent::AssignedConsumerControlCompleted(_)
            )
            | (
                ExpectedEvent::AssignedConsumerEventObserved(_),
                AdapterEvent::AssignedConsumerEventObserved(_)
            )
            | (
                ExpectedEvent::ReceiveCompleted(_),
                AdapterEvent::ReceiveCompleted { .. }
            )
            | (
                ExpectedEvent::AssignedRecordTransferCompleted(_),
                AdapterEvent::OperationAccepted { .. }
                    | AdapterEvent::OperationTerminal { .. }
                    | AdapterEvent::AssignedRecordTransferCompleted(_)
            )
            | (
                ExpectedEvent::AssignedConsumerClosed(_),
                AdapterEvent::AssignedConsumerClosed { .. }
            )
            | (
                ExpectedEvent::GroupConsumerCreated(_),
                AdapterEvent::GroupConsumerCreated { .. }
            )
            | (
                ExpectedEvent::GroupReceiveCompleted(_),
                AdapterEvent::GroupReceiveCompleted { .. }
            )
            | (
                ExpectedEvent::GroupAssignmentsObserved(_),
                AdapterEvent::GroupAssignmentsObserved(_)
            )
            | (
                ExpectedEvent::GroupReceiveSetCompleted(_),
                AdapterEvent::GroupReceiveSetCompleted(_)
            )
            | (
                ExpectedEvent::GroupConsumerControlCompleted(_),
                AdapterEvent::GroupConsumerControlCompleted(_)
            )
            | (
                ExpectedEvent::GroupConsumerShutdownCompleted(_),
                AdapterEvent::GroupConsumerShutdownCompleted(_)
            )
            | (
                ExpectedEvent::GroupConsumerClosed(_),
                AdapterEvent::GroupConsumerClosed { .. }
            )
            | (
                ExpectedEvent::GroupConsumerAbandoned(_),
                AdapterEvent::GroupConsumerAbandoned(_)
            )
    )
}

fn same_extended_event_family(expected: &ExpectedEvent, event: &AdapterEvent) -> bool {
    matches!(
        (expected, event),
        (
            ExpectedEvent::ShareConsumerCreated(_),
            AdapterEvent::ShareConsumerCreated { .. }
        ) | (
            ExpectedEvent::ShareReceiveCompleted(_),
            AdapterEvent::ShareReceiveCompleted { .. }
        ) | (
            ExpectedEvent::ShareAcknowledgementCompleted(_),
            AdapterEvent::ShareAcknowledgementCompleted { .. }
        ) | (
            ExpectedEvent::ShareBatchDropped(_),
            AdapterEvent::ShareBatchDropped { .. }
        ) | (
            ExpectedEvent::ShareConsumerClosed(_),
            AdapterEvent::ShareConsumerClosed { .. }
        ) | (
            ExpectedEvent::TransactionalProducerCreated(_),
            AdapterEvent::TransactionalProducerCreated(_)
        ) | (
            ExpectedEvent::TransactionCompleted(_),
            AdapterEvent::OperationAccepted { .. }
                | AdapterEvent::OperationRejected { .. }
                | AdapterEvent::OperationTerminal { .. }
                | AdapterEvent::ProducersDescribed(_)
                | AdapterEvent::TransactionCompleted { .. }
                | AdapterEvent::TransactionalTransformCompleted(_)
        ) | (
            ExpectedEvent::TransactionFenceCompleted { .. },
            AdapterEvent::OperationAccepted { .. }
                | AdapterEvent::OperationRejected { .. }
                | AdapterEvent::OperationTerminal { .. }
                | AdapterEvent::TransactionalProducerCreated(_)
                | AdapterEvent::TransactionFenceCompleted { .. }
        ) | (
            ExpectedEvent::TransactionalProducerClosed(_),
            AdapterEvent::TransactionalProducerClosed { .. }
        ) | (
            ExpectedEvent::FlushCompleted(_),
            AdapterEvent::FlushCompleted { .. }
        ) | (
            ExpectedEvent::ProducerClosed(_),
            AdapterEvent::ProducerClosed { .. }
        ) | (
            ExpectedEvent::ClientShutdown(_),
            AdapterEvent::ClientShutdown { .. }
        ) | (ExpectedEvent::Finished, AdapterEvent::Finished)
            | (ExpectedEvent::Aborted, AdapterEvent::Aborted)
    )
}
