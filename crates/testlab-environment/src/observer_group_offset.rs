//! Consumer-group offset observation queries one exact partition without joining the group.

use std::thread;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use testlab_schema::{
    BrokerConsumerGroupOffset, BrokerStateObservation, ListConsumerGroupOffsetsAction,
    ListConsumerGroupOffsetsCommand, OperationId, RunId, Scenario, ScenarioAction,
};

use crate::observer::{ObserverRequest, remaining};
use crate::observer_admin::AdminObserverRequest;
use crate::observer_admin_target::OffsetTarget;
use crate::observer_error::ObserverError;
use crate::security::ClientSecurity;

const POLL_SLICE: Duration = Duration::from_millis(50);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ConsumerGroupOffsetTarget {
    pub(super) operation_id: OperationId,
    pub(super) group_id: String,
    pub(super) topic: String,
    pub(super) partition: i32,
}

pub(super) fn capture(
    request: ObserverRequest<'_>,
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    targets(request.scenario, request.issued_group_offset_commands)
        .into_iter()
        .enumerate()
        .map(|(observation, target)| {
            let observation =
                u64::try_from(observation).map_err(|_| ObserverError::ObservationOverflow)?;
            capture_target(
                observation,
                request.endpoint,
                request.run_id,
                request.deadline,
                request.security,
                &target,
            )
        })
        .collect()
}

pub(super) fn targets(
    scenario: &Scenario,
    issued_commands: &[ListConsumerGroupOffsetsCommand],
) -> Vec<ConsumerGroupOffsetTarget> {
    scenario
        .steps
        .iter()
        .filter_map(|step| match &step.action {
            ScenarioAction::ListConsumerGroupOffsets(action)
                if exact_issued_command(action, issued_commands) =>
            {
                Some(ConsumerGroupOffsetTarget {
                    operation_id: action.operation_id.clone(),
                    group_id: action.group_id.clone(),
                    topic: action.topic.clone(),
                    partition: action.partition,
                })
            }
            _ => None,
        })
        .collect()
}

fn exact_issued_command(
    action: &ListConsumerGroupOffsetsAction,
    issued_commands: &[ListConsumerGroupOffsetsCommand],
) -> bool {
    let mut candidates = issued_commands
        .iter()
        .filter(|command| command.operation_id == action.operation_id);
    let Some(candidate) = candidates.next() else {
        return false;
    };
    if candidates.next().is_some() {
        return false;
    }
    candidate
        == &ListConsumerGroupOffsetsCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            group_id: action.group_id.clone(),
            topic: action.topic.clone(),
            partition: action.partition,
            require_stable: action.require_stable,
            timeout_ms: action.timeout_ms,
        }
}

fn capture_target(
    observation: u64,
    endpoint: &str,
    run_id: &RunId,
    deadline: std::time::Instant,
    security: &ClientSecurity,
    target: &ConsumerGroupOffsetTarget,
) -> Result<BrokerStateObservation, ObserverError> {
    remaining(deadline)?;
    let consumer = consumer(endpoint, run_id, observation, &target.group_id, security)?;
    capture_with_consumer(observation, deadline, &consumer, target)
}

pub(super) fn capture_admin_target(
    request: AdminObserverRequest<'_>,
    target: &OffsetTarget,
) -> Result<BrokerStateObservation, ObserverError> {
    let exact = ConsumerGroupOffsetTarget {
        operation_id: target.operation_id.clone(),
        group_id: target.group_id.clone(),
        topic: target.topic.clone(),
        partition: target.partition,
    };
    let consumer = consumer(
        request.endpoint,
        request.run_id,
        request.first_observation,
        &target.group_id,
        request.security,
    )?;
    loop {
        let observed = capture_with_consumer(
            request.first_observation,
            request.deadline,
            &consumer,
            &exact,
        )?;
        if !target.poll_expected || observed_offset(&observed)? == target.expected_offset {
            return Ok(observed);
        }
        let wait = request
            .deadline
            .saturating_duration_since(std::time::Instant::now());
        if wait.is_zero() {
            return Err(ObserverError::Deadline);
        }
        thread::sleep(POLL_SLICE.min(wait));
    }
}

fn capture_with_consumer(
    observation: u64,
    deadline: std::time::Instant,
    consumer: &BaseConsumer,
    target: &ConsumerGroupOffsetTarget,
) -> Result<BrokerStateObservation, ObserverError> {
    let mut partitions = TopicPartitionList::new();
    partitions.add_partition(&target.topic, target.partition);
    let offsets = consumer.committed_offsets(partitions, remaining(deadline)?)?;
    normalize_response(observation, target, &offsets)
}

fn consumer(
    endpoint: &str,
    run_id: &RunId,
    observation: u64,
    group_id: &str,
    security: &ClientSecurity,
) -> Result<BaseConsumer, ObserverError> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", endpoint)
        .set(
            "client.id",
            format!("testlab-state-observer-{run_id}-{observation}"),
        )
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false");
    security.configure(&mut config);
    config.create().map_err(ObserverError::Kafka)
}

pub(super) fn normalize_response(
    observation: u64,
    target: &ConsumerGroupOffsetTarget,
    offsets: &TopicPartitionList,
) -> Result<BrokerStateObservation, ObserverError> {
    let mut elements = offsets.elements().into_iter();
    let element = elements
        .next()
        .ok_or_else(|| invalid(target, "returned no partition"))?;
    if elements.next().is_some() {
        return Err(invalid(target, "returned more than one partition"));
    }
    element.error()?;
    if element.topic() != target.topic || element.partition() != target.partition {
        return Err(invalid(
            target,
            format!(
                "returned unexpected partition {}:{}",
                element.topic(),
                element.partition()
            ),
        ));
    }
    let offset = normalize_offset(target, element.offset())?;
    Ok(BrokerStateObservation::ConsumerGroupOffset(
        BrokerConsumerGroupOffset {
            observation,
            operation_id: target.operation_id.clone(),
            group_id: target.group_id.clone(),
            topic: target.topic.clone(),
            partition: target.partition,
            offset,
        },
    ))
}

fn normalize_offset(
    target: &ConsumerGroupOffsetTarget,
    offset: Offset,
) -> Result<Option<i64>, ObserverError> {
    match offset {
        Offset::Invalid => Ok(None),
        Offset::Offset(offset) if offset >= 0 => Ok(Some(offset)),
        unsupported => Err(invalid(
            target,
            format!("returned unsupported offset {unsupported:?}"),
        )),
    }
}

fn invalid(target: &ConsumerGroupOffsetTarget, detail: impl std::fmt::Display) -> ObserverError {
    ObserverError::InvalidBrokerState(format!(
        "consumer group {} offset for {}:{} {detail}",
        target.group_id, target.topic, target.partition
    ))
}

fn observed_offset(observation: &BrokerStateObservation) -> Result<Option<i64>, ObserverError> {
    match observation {
        BrokerStateObservation::ConsumerGroupOffset(value) => Ok(value.offset),
        _ => Err(ObserverError::InvalidBrokerState(
            "group-offset query returned a different broker-state kind".to_owned(),
        )),
    }
}
