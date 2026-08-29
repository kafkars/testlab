//! Read-only admin verification joins public results to independent broker observations.

use std::collections::BTreeSet;

use testlab_schema::{
    AdminOffsetPosition, BrokerObservation, OperationId, ScenarioAction, Violation,
};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::{
    HistoryIndex, IndexedOffsetList, IndexedPartitionOffsetsObservation, IndexedTopicDescription,
    IndexedTopicObservation, IndexedTopicsList,
};
use crate::support::violation;

pub(crate) fn verify_discovery_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    _observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) -> bool {
    let command_window = index.admin_command_window(action);
    match action {
        ScenarioAction::DescribeTopic(action) if action.expected_error_code.is_none() => {
            let Some(expected_partitions) = action.expected_partitions.as_deref() else {
                return false;
            };
            verify_description(
                &action.operation_id,
                &action.topic,
                expected_partitions,
                index.topics_described.get(&action.operation_id),
                index.topics_observed.get(&action.operation_id),
                command_window,
                violations,
            );
        }
        ScenarioAction::ListTopics(action) => verify_topics(
            &action.operation_id,
            &action.required_topics,
            index.topics_listed.get(&action.operation_id),
            index.topics_observed.get(&action.operation_id),
            command_window,
            violations,
        ),
        ScenarioAction::ListOffsets(action) if action.expected_error_code.is_none() => {
            let Some(expected_offset) = action.expected_offset else {
                return false;
            };
            verify_offset(
                OffsetExpectation {
                    operation_id: &action.operation_id,
                    topic: &action.topic,
                    partition: action.partition,
                    position: action.position,
                    expected_offset,
                },
                index.offsets_listed.get(&action.operation_id),
                index.partition_offsets_observed.get(&action.operation_id),
                command_window,
                violations,
            );
        }
        _ => return false,
    }
    true
}

fn verify_description(
    operation_id: &OperationId,
    topic: &str,
    expected_partitions: &[i32],
    completions: Option<&Vec<IndexedTopicDescription>>,
    independent: Option<&Vec<IndexedTopicObservation>>,
    command_window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let public = completions
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let public_matches = public.is_some_and(|value| {
        value.topic == topic
            && value.partitions == expected_partitions
            && public_after_command(command_window, value.history_sequence)
    });
    let independent_matches = independent.is_some_and(|values| {
        values.len() == 1
            && values.first().is_some_and(|value| {
                value.topic == topic
                    && value.exists
                    && value.partitions == expected_partitions
                    && public.is_some_and(|public| {
                        immediate_after_public(
                            command_window,
                            public.history_sequence,
                            value.history_sequence,
                        )
                    })
            })
    });
    if public_matches && independent_matches {
        return;
    }
    violations.push(violation(
        "ADMIN-003",
        format!(
            "admin operation {operation_id} expected public and independent descriptions of {topic} partitions {expected_partitions:?}"
        ),
        Some(operation_id.clone()),
        evidence(
            completions.into_iter().flatten().map(|value| value.history_sequence),
            std::iter::empty(),
        ),
    ));
    append_state_evidence(violations, independent);
}

fn verify_topics(
    operation_id: &OperationId,
    required_topics: &[String],
    completions: Option<&Vec<IndexedTopicsList>>,
    independent: Option<&Vec<IndexedTopicObservation>>,
    command_window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let required: BTreeSet<&str> = required_topics.iter().map(String::as_str).collect();
    let independently_present: BTreeSet<&str> = independent
        .into_iter()
        .flatten()
        .filter(|value| value.exists && required.contains(value.topic.as_str()))
        .map(|value| value.topic.as_str())
        .collect();
    let public = completions
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let independent_matches = independent.is_some_and(|values| {
        values.len() == required.len()
            && independently_present.len() == required.len()
            && public.is_some_and(|public| {
                values.iter().all(|value| {
                    immediate_after_public(
                        command_window,
                        public.history_sequence,
                        value.history_sequence,
                    )
                })
            })
    });
    let public_matches = public.is_some_and(|value| {
        strictly_sorted(&value.topics)
            && required_topics
                .iter()
                .all(|topic| value.topics.binary_search(topic).is_ok())
            && public_after_command(command_window, value.history_sequence)
    });
    if public_matches && independent_matches {
        return;
    }
    violations.push(violation(
        "ADMIN-004",
        format!(
            "admin operation {operation_id} expected one sorted topic list containing independently present topics {required_topics:?}; independently present {independently_present:?}"
        ),
        Some(operation_id.clone()),
        evidence(
            completions.into_iter().flatten().map(|value| value.history_sequence),
            std::iter::empty(),
        ),
    ));
    append_state_evidence(violations, independent);
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
    independent: Option<&Vec<IndexedPartitionOffsetsObservation>>,
    command_window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let OffsetExpectation {
        operation_id,
        topic,
        partition,
        position,
        expected_offset,
    } = expected;
    let public = completions
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let state = independent
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let independent_offset = state.map(|value| match position {
        AdminOffsetPosition::Earliest => value.low_watermark,
        AdminOffsetPosition::Latest => value.high_watermark,
    });
    let public_matches = public.is_some_and(|value| {
        value.topic == topic
            && value.partition == partition
            && value.offset == Some(expected_offset)
            && value.offset == independent_offset
            && public_after_command(command_window, value.history_sequence)
    });
    let independent_matches = state.is_some_and(|value| {
        value.topic == topic
            && value.partition == partition
            && value.low_watermark >= 0
            && value.high_watermark >= value.low_watermark
            && independent_offset == Some(expected_offset)
            && public.is_some_and(|public| {
                immediate_after_public(
                    command_window,
                    public.history_sequence,
                    value.history_sequence,
                )
            })
    });
    if public_matches && independent_matches {
        return;
    }
    violations.push(violation(
        "ADMIN-005",
        format!(
            "admin operation {operation_id} expected {position:?} offset {expected_offset} for {topic}[{partition}], independently derived {independent_offset:?}"
        ),
        Some(operation_id.clone()),
        offset_evidence(completions, independent),
    ));
}

fn offset_evidence(
    public: Option<&Vec<IndexedOffsetList>>,
    independent: Option<&Vec<IndexedPartitionOffsetsObservation>>,
) -> Vec<String> {
    public
        .into_iter()
        .flatten()
        .map(|value| format!("history:{}", value.history_sequence))
        .chain(
            independent
                .into_iter()
                .flatten()
                .map(|value| format!("broker-state-observation:{}", value.observation)),
        )
        .collect()
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

fn append_state_evidence(
    violations: &mut [Violation],
    observations: Option<&Vec<IndexedTopicObservation>>,
) {
    let Some(violation) = violations.last_mut() else {
        return;
    };
    violation.evidence.extend(
        observations
            .into_iter()
            .flatten()
            .map(|value| format!("broker-state-observation:{}", value.observation)),
    );
}
