//! Broker-state completions retain every stable target identity.

use testlab_schema::{AdapterEvent, AdminProducersDescription};

use crate::run_error::RunFailure;
use crate::runner_protocol::{EventDisposition, ExpectedEvent};

pub(super) fn classify(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    if let (
        ExpectedEvent::ReplicaLogDirsAltered(operation_id),
        AdapterEvent::ReplicaLogDirsAltered(actual),
    ) = (expected, event)
    {
        return Some(if operation_id == &actual.operation_id {
            Ok(EventDisposition::Complete)
        } else {
            Err(mismatch(event, expected))
        });
    }
    if let Some(result) = classify_replica_log_dirs(expected, event) {
        return Some(result);
    }
    if let Some(result) = classify_log_dirs(expected, event) {
        return Some(result);
    }
    let (
        ExpectedEvent::ProducerStatesDescribed {
            operation_id,
            topic,
            partition,
        },
        AdapterEvent::ProducersDescribed(AdminProducersDescription {
            operation_id: actual_operation,
            topic: actual_topic,
            partition: actual_partition,
            ..
        }),
    ) = (expected, event)
    else {
        return None;
    };
    if operation_id == actual_operation && topic == actual_topic && partition == actual_partition {
        Some(Ok(EventDisposition::Complete))
    } else {
        Some(Err(mismatch(event, expected)))
    }
}

fn classify_replica_log_dirs(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    let (
        ExpectedEvent::ReplicaLogDirsDescribed(operation_id, topic, partition),
        AdapterEvent::ReplicaLogDirsDescribed(actual),
    ) = (expected, event)
    else {
        return None;
    };
    if operation_id == &actual.operation_id
        && topic == &actual.topic
        && partition == &actual.partition
    {
        Some(Ok(EventDisposition::Complete))
    } else {
        Some(Err(mismatch(event, expected)))
    }
}

fn mismatch(event: &AdapterEvent, expected: &ExpectedEvent) -> RunFailure {
    RunFailure::protocol(
        "event_identity_mismatch",
        format!("event {event:?} does not match expected {expected:?}"),
    )
}

fn classify_log_dirs(
    expected: &ExpectedEvent,
    event: &AdapterEvent,
) -> Option<Result<EventDisposition, RunFailure>> {
    let (
        ExpectedEvent::LogDirsDescribed {
            operation_id,
            topic,
            partition,
        },
        AdapterEvent::LogDirsDescribed(actual),
    ) = (expected, event)
    else {
        return None;
    };
    if operation_id == &actual.operation_id
        && topic == &actual.topic
        && partition == &actual.partition
    {
        Some(Ok(EventDisposition::Complete))
    } else {
        Some(Err(RunFailure::protocol(
            "event_identity_mismatch",
            format!("event {event:?} does not match expected {expected:?}"),
        )))
    }
}
