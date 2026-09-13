//! Read-only admin verification joins public results to independent broker observations.
use testlab_schema::{
    AdminOffsetSelector, AdminReadIsolation, BrokerObservation, OperationId, ScenarioAction,
    Violation,
};
#[path = "admin_timestamp_offset.rs"]
mod timestamp_offset;
#[path = "admin_topic_listing.rs"]
mod topic_listing;
#[path = "admin_topic_pagination.rs"]
mod topic_pagination;
use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::{
    HistoryIndex, IndexedOffsetList, IndexedPartitionOffsetsObservation, IndexedTopicDescription,
    IndexedTopicObservation,
};
use crate::support::violation;

pub(crate) fn verify_discovery_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) -> bool {
    let command_window = index.admin_command_window(action);
    match action {
        ScenarioAction::DescribeTopic(action) if action.expected_error_code.is_none() => {
            let Some(expected_partitions) = action.expected_partitions.as_deref() else {
                return false;
            };
            verify_description(
                action,
                expected_partitions,
                index.topics_described.get(&action.operation_id),
                index.topics_observed.get(&action.operation_id),
                command_window,
                violations,
            );
        }
        ScenarioAction::ListTopics(action) => topic_listing::verify(
            &action.operation_id,
            &action.expected_topics,
            action.include_authorized_operations,
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
                    contract: offset_contract(action.read_isolation, action.position),
                    timestamp_millis: action.timestamp_millis,
                    expected_offset,
                },
                index.offsets_listed.get(&action.operation_id),
                index.partition_offsets_observed.get(&action.operation_id),
                command_window,
                observations,
                violations,
            );
        }
        _ => return false,
    }
    true
}

fn verify_description(
    action: &testlab_schema::DescribeTopicAction,
    expected_partitions: &[i32],
    completions: Option<&Vec<IndexedTopicDescription>>,
    independent: Option<&Vec<IndexedTopicObservation>>,
    command_window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let operation_id = &action.operation_id;
    let topic = &action.topic;
    let public = completions
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    if let Some(public) = public {
        crate::admin_topic::topology::verify_single(
            operation_id,
            action.api,
            &public.partitions,
            &public.partition_details,
            &public.pages,
            public.history_sequence,
            violations,
        );
    }
    let public_matches = public.is_some_and(|value| {
        value.topic == topic
            && value.partitions == expected_partitions
            && topic_pagination::matches(action, &value.pages)
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

#[derive(Clone, Copy)]
pub(super) struct OffsetExpectation<'a> {
    operation_id: &'a OperationId,
    topic: &'a str,
    partition: i32,
    pub(super) position: AdminOffsetSelector,
    pub(super) contract: &'static str,
    pub(super) timestamp_millis: Option<i64>,
    pub(super) expected_offset: i64,
}

fn verify_offset(
    expected: OffsetExpectation<'_>,
    completions: Option<&Vec<IndexedOffsetList>>,
    independent: Option<&Vec<IndexedPartitionOffsetsObservation>>,
    command_window: Option<AdminCommandWindow>,
    observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) {
    if timestamp_offset::applies(expected.position) {
        timestamp_offset::verify(
            expected,
            completions,
            independent,
            command_window,
            observations,
            violations,
        );
        return;
    }
    let OffsetExpectation {
        operation_id,
        topic,
        partition,
        position,
        contract,
        timestamp_millis: _,
        expected_offset,
    } = expected;
    let public = completions
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let state = independent
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let independent_offset = state.map(|value| match position {
        AdminOffsetSelector::Earliest => value.low_watermark,
        AdminOffsetSelector::Latest => value.high_watermark,
        _ => unreachable!("record-timestamp selectors return above"),
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
        contract,
        format!(
            "admin operation {operation_id} expected {position:?} offset {expected_offset} for {topic}[{partition}], independently derived {independent_offset:?}"
        ),
        Some(operation_id.clone()),
        offset_evidence(completions, independent),
    ));
}

const fn offset_contract(
    read_isolation: AdminReadIsolation,
    position: AdminOffsetSelector,
) -> &'static str {
    if matches!(read_isolation, AdminReadIsolation::ReadUncommitted) {
        "ADMIN-088"
    } else {
        match position {
            AdminOffsetSelector::Timestamp => "ADMIN-077",
            AdminOffsetSelector::MaxTimestamp => "ADMIN-078",
            AdminOffsetSelector::Earliest | AdminOffsetSelector::Latest => "ADMIN-005",
        }
    }
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
