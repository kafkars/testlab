//! A separate librdkafka consumer snapshots exact records at broker watermarks.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::types::RDKafkaErrorCode;
use testlab_schema::{BrokerObservation, OperationId, RunId, Scenario, ScenarioAction};

use crate::observer_error::ObserverError;
use crate::observer_record::{CapturedRecord, normalize};
use crate::security::ClientSecurity;

const POLL_SLICE: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Debug)]
pub(super) struct ObserverRequest<'a> {
    pub(super) endpoint: &'a str,
    pub(super) run_id: &'a RunId,
    pub(super) scenario: &'a Scenario,
    pub(super) issued_operations: &'a BTreeSet<OperationId>,
    pub(super) timeout: Duration,
    pub(super) security: &'a ClientSecurity,
}

#[derive(Clone, Copy, Debug)]
struct Cursor {
    next: i64,
    high: i64,
}

type PartitionKey = (String, i32);
type Cursors = BTreeMap<PartitionKey, Cursor>;
type Assignment = (TopicPartitionList, Cursors);

pub(super) fn capture(
    request: ObserverRequest<'_>,
) -> Result<Vec<BrokerObservation>, ObserverError> {
    let targets = targets(request.scenario, request.issued_operations);
    if targets.is_empty() {
        return Ok(Vec::new());
    }
    let deadline = Instant::now()
        .checked_add(request.timeout)
        .ok_or(ObserverError::DeadlineOverflow)?;
    let consumer = consumer(request.endpoint, request.run_id, request.security)?;
    let (assignment, mut cursors) = assignment(&consumer, targets, deadline)?;
    consumer.assign(&assignment)?;
    poll_snapshot(&consumer, &mut cursors, deadline)
}

fn consumer(
    endpoint: &str,
    run_id: &RunId,
    security: &ClientSecurity,
) -> Result<BaseConsumer, ObserverError> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", endpoint)
        .set("group.id", format!("testlab-observer-{run_id}"))
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "true")
        .set("isolation.level", "read_committed");
    security.configure(&mut config);
    config.create().map_err(ObserverError::Kafka)
}

pub(super) fn targets(
    scenario: &Scenario,
    issued_operations: &BTreeSet<OperationId>,
) -> BTreeSet<(String, i32)> {
    let mut targets = BTreeSet::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::Send {
                operation_id,
                record,
                ..
            } if issued_operations.contains(operation_id) => {
                targets.insert((record.topic.clone(), record.partition));
            }
            ScenarioAction::SendBatch { operations, .. }
            | ScenarioAction::ExecuteTransaction { operations, .. } => targets.extend(
                operations
                    .iter()
                    .filter(|operation| issued_operations.contains(&operation.operation_id))
                    .map(|operation| (operation.record.topic.clone(), operation.record.partition)),
            ),
            ScenarioAction::FenceTransaction { operation, .. }
                if issued_operations.contains(&operation.operation_id) =>
            {
                targets.insert((operation.record.topic.clone(), operation.record.partition));
            }
            _ => {}
        }
    }
    targets
}

fn assignment(
    consumer: &BaseConsumer,
    targets: BTreeSet<(String, i32)>,
    deadline: Instant,
) -> Result<Assignment, ObserverError> {
    let mut assignment = TopicPartitionList::new();
    let mut cursors = BTreeMap::new();
    for (topic, partition) in targets {
        let (low, high) = consumer.fetch_watermarks(&topic, partition, remaining(deadline)?)?;
        if high < low {
            return Err(ObserverError::InvalidRecord(format!(
                "watermarks for {topic}:{partition} are {low}..{high}"
            )));
        }
        assignment.add_partition_offset(&topic, partition, Offset::Offset(low))?;
        cursors.insert((topic, partition), Cursor { next: low, high });
    }
    Ok((assignment, cursors))
}

fn poll_snapshot(
    consumer: &BaseConsumer,
    cursors: &mut Cursors,
    deadline: Instant,
) -> Result<Vec<BrokerObservation>, ObserverError> {
    let mut observations = Vec::new();
    let mut seen = BTreeSet::new();
    while cursors.values().any(|cursor| cursor.next < cursor.high) {
        match consumer.poll(POLL_SLICE.min(remaining(deadline)?)) {
            Some(Ok(message)) => {
                let key = (message.topic().to_owned(), message.partition());
                let cursor = cursors
                    .get_mut(&key)
                    .ok_or_else(|| ObserverError::UnexpectedPartition(key.0.clone(), key.1))?;
                if message.offset() >= cursor.high {
                    return Err(ObserverError::InvalidRecord(format!(
                        "offset {} exceeded snapshot watermark {} for {}:{}",
                        message.offset(),
                        cursor.high,
                        key.0,
                        key.1
                    )));
                }
                cursor.next = cursor.next.max(
                    message
                        .offset()
                        .checked_add(1)
                        .ok_or(ObserverError::OffsetOverflow)?,
                );
                if seen.insert((key.0, key.1, message.offset())) {
                    let ordinal = u64::try_from(observations.len())
                        .map_err(|_| ObserverError::ObservationOverflow)?;
                    observations.push(normalize_message(ordinal, &message)?);
                }
            }
            Some(Err(KafkaError::PartitionEOF(_))) | None => {}
            Some(Err(error)) if is_transient(&error) => {}
            Some(Err(error)) => return Err(ObserverError::Kafka(error)),
        }
        advance_positions(consumer, cursors)?;
    }
    Ok(observations)
}

fn advance_positions(consumer: &BaseConsumer, cursors: &mut Cursors) -> Result<(), ObserverError> {
    let positions = consumer.position()?;
    for element in positions.elements() {
        let Offset::Offset(position) = element.offset() else {
            continue;
        };
        let key = (element.topic().to_owned(), element.partition());
        if let Some(cursor) = cursors.get_mut(&key) {
            cursor.next = cursor.next.max(position.min(cursor.high));
        }
    }
    Ok(())
}

pub(super) fn is_transient(error: &KafkaError) -> bool {
    matches!(
        error,
        KafkaError::MessageConsumption(
            RDKafkaErrorCode::BrokerTransportFailure
                | RDKafkaErrorCode::AllBrokersDown
                | RDKafkaErrorCode::OperationTimedOut
        )
    )
}

fn normalize_message(
    observation: u64,
    message: &rdkafka::message::BorrowedMessage<'_>,
) -> Result<BrokerObservation, ObserverError> {
    let headers = message
        .headers()
        .map(|headers| {
            headers
                .iter()
                .map(|header| (header.key, header.value))
                .collect()
        })
        .unwrap_or_default();
    normalize(
        observation,
        CapturedRecord {
            topic: message.topic(),
            partition: message.partition(),
            offset: message.offset(),
            key: message.key(),
            value: message.payload(),
            headers,
        },
    )
}

fn remaining(deadline: Instant) -> Result<Duration, ObserverError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(ObserverError::Deadline)
    } else {
        Ok(remaining)
    }
}
