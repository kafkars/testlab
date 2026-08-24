//! Read-only admin verification joins public results to independent broker observations.

use std::collections::BTreeSet;

use testlab_schema::{
    AdminOffsetPosition, BrokerObservation, OperationId, ScenarioAction, Violation,
};

use crate::index::{HistoryIndex, IndexedOffsetList, IndexedTopicDescription, IndexedTopicsList};
use crate::support::violation;

pub(crate) fn verify_discovery_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) -> bool {
    match action {
        ScenarioAction::DescribeTopic(action) => verify_description(
            &action.operation_id,
            &action.topic,
            &action.expected_partitions,
            index.topics_described.get(&action.operation_id),
            observations,
            violations,
        ),
        ScenarioAction::ListTopics(action) => verify_topics(
            &action.operation_id,
            &action.required_topics,
            index.topics_listed.get(&action.operation_id),
            observations,
            violations,
        ),
        ScenarioAction::ListOffsets(action) => verify_offset(
            OffsetExpectation {
                operation_id: &action.operation_id,
                topic: &action.topic,
                partition: action.partition,
                position: action.position,
                expected_offset: action.expected_offset,
            },
            index.offsets_listed.get(&action.operation_id),
            observations,
            violations,
        ),
        _ => return false,
    }
    true
}

fn verify_description(
    operation_id: &OperationId,
    topic: &str,
    expected_partitions: &[i32],
    completions: Option<&Vec<IndexedTopicDescription>>,
    observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) {
    let observed_partitions: BTreeSet<i32> = observations
        .iter()
        .filter(|observation| observation.record.topic == topic)
        .map(|observation| observation.record.partition)
        .collect();
    let public_matches = completions.is_some_and(|values| {
        values.len() == 1
            && values.first().is_some_and(|value| {
                value.topic == topic && value.partitions == expected_partitions
            })
    });
    let independently_exercised = expected_partitions
        .iter()
        .all(|partition| observed_partitions.contains(partition));
    if public_matches && independently_exercised {
        return;
    }
    violations.push(violation(
        "ADMIN-003",
        format!(
            "admin operation {operation_id} expected one exact description of {topic} partitions {expected_partitions:?}; independently exercised {observed_partitions:?}"
        ),
        Some(operation_id.clone()),
        evidence(
            completions.into_iter().flatten().map(|value| value.history_sequence),
            observations.iter().filter(|value| value.record.topic == topic),
        ),
    ));
}

fn verify_topics(
    operation_id: &OperationId,
    required_topics: &[String],
    completions: Option<&Vec<IndexedTopicsList>>,
    observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) {
    let required: BTreeSet<&str> = required_topics.iter().map(String::as_str).collect();
    let independent: BTreeSet<&str> = observations
        .iter()
        .map(|observation| observation.record.topic.as_str())
        .filter(|topic| required.contains(topic))
        .collect();
    let public_matches = completions.is_some_and(|values| {
        values.len() == 1
            && values.first().is_some_and(|value| {
                strictly_sorted(&value.topics)
                    && required_topics
                        .iter()
                        .all(|topic| value.topics.binary_search(topic).is_ok())
            })
    });
    if public_matches && independent.len() == required.len() {
        return;
    }
    violations.push(violation(
        "ADMIN-004",
        format!(
            "admin operation {operation_id} expected one sorted topic list containing independently observed topics {required_topics:?}; independently observed {independent:?}"
        ),
        Some(operation_id.clone()),
        evidence(
            completions.into_iter().flatten().map(|value| value.history_sequence),
            observations
                .iter()
                .filter(|value| required.contains(value.record.topic.as_str())),
        ),
    ));
}

#[derive(Clone, Copy)]
struct OffsetExpectation<'a> {
    operation_id: &'a OperationId,
    topic: &'a str,
    partition: i32,
    position: AdminOffsetPosition,
    expected_offset: i64,
}

fn verify_offset(
    expected: OffsetExpectation<'_>,
    completions: Option<&Vec<IndexedOffsetList>>,
    observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) {
    let OffsetExpectation {
        operation_id,
        topic,
        partition,
        position,
        expected_offset,
    } = expected;
    let matching: Vec<&BrokerObservation> = observations
        .iter()
        .filter(|value| value.record.topic == topic && value.record.partition == partition)
        .collect();
    let independent_offset = match position {
        AdminOffsetPosition::Latest => matching
            .iter()
            .map(|value| value.offset)
            .max()
            .and_then(|offset| offset.checked_add(1)),
    };
    let public_matches = completions.is_some_and(|values| {
        values.len() == 1
            && values.first().is_some_and(|value| {
                value.topic == topic
                    && value.partition == partition
                    && value.offset == Some(expected_offset)
                    && value.offset == independent_offset
            })
    });
    if public_matches && independent_offset.is_some() {
        return;
    }
    violations.push(violation(
        "ADMIN-005",
        format!(
            "admin operation {operation_id} expected {position:?} offset {expected_offset} for {topic}[{partition}], independently derived {independent_offset:?}"
        ),
        Some(operation_id.clone()),
        evidence(
            completions.into_iter().flatten().map(|value| value.history_sequence),
            matching.into_iter(),
        ),
    ));
}

fn strictly_sorted(values: &[String]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn evidence<'a>(
    history: impl Iterator<Item = u64>,
    observations: impl Iterator<Item = &'a BrokerObservation>,
) -> Vec<String> {
    history
        .map(|sequence| format!("history:{sequence}"))
        .chain(observations.map(|value| format!("broker-observation:{}", value.observation)))
        .collect()
}
