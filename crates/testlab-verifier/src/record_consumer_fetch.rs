//! Assigned Fetch evidence joins one public lease to independent broker facts.

use std::collections::BTreeMap;

use testlab_schema::{
    AdminOffsetPosition, BrokerObservation, OperationId, Scenario, ScenarioAction, TopicSelection,
    Violation,
};

use crate::index::{
    HistoryIndex, IndexedPartitionOffsetsObservation, IndexedTopicIdentityObservation,
};
use crate::support::violation;

pub(super) fn verify(
    scenario: &Scenario,
    index: &HistoryIndex,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    for step in &scenario.steps {
        let ScenarioAction::Receive {
            receive_id,
            expected_operation_id,
            observe_fetch_evidence: true,
            ..
        } = &step.action
        else {
            continue;
        };
        if !index.action_issued(&step.action) {
            continue;
        }
        verify_receive(
            scenario,
            receive_id,
            expected_operation_id,
            index,
            observed,
            violations,
        );
    }
}

fn verify_receive(
    scenario: &Scenario,
    receive_id: &OperationId,
    expected_operation_id: &OperationId,
    index: &HistoryIndex,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    let receive = one(index.receives.get(receive_id));
    let record = one(observed.get(expected_operation_id));
    let identity = record.and_then(|record| topic_identity(scenario, record, index));
    let offsets = record.and_then(|record| partition_offsets(scenario, record, index));
    let matches = receive.zip(record).zip(identity).zip(offsets).is_some_and(
        |(((receive, record), identity), offsets)| {
            receive.fetch_evidence.as_ref().is_some_and(|evidence| {
                let next_offset = record.offset.checked_add(1);
                evidence.topic == record.record.topic
                    && evidence.topic_uuid == identity.topic_id
                    && identity.topic == record.record.topic
                    && identity.partitions.contains(&record.record.partition)
                    && evidence.partition == record.record.partition
                    && offsets.topic == record.record.topic
                    && offsets.partition == record.record.partition
                    && offsets.low_watermark <= record.offset
                    && record.offset < offsets.high_watermark
                    && evidence.requested_offset == offsets.low_watermark
                    && Some(evidence.next_offset) == next_offset
                    && evidence.checkpoint_next_offset == evidence.next_offset
                    && evidence.log_start_offset == Some(offsets.low_watermark)
                    && evidence.last_stable_offset == Some(offsets.high_watermark)
                    && evidence.high_watermark == Some(offsets.high_watermark)
                    && evidence.retained_bytes > 0
                    && identity.history_sequence < receive.history_sequence
                    && offsets.history_sequence < receive.history_sequence
            })
        },
    );
    if matches {
        return;
    }
    violations.push(violation(
        "CONS-022",
        format!(
            "assigned receive {receive_id} did not retain one Fetch lease matching independent topic identity, watermarks, record progress, and public checkpoint"
        ),
        Some(receive_id.clone()),
        evidence(receive, record, identity, offsets),
    ));
}

fn topic_identity<'a>(
    scenario: &'a Scenario,
    record: &BrokerObservation,
    index: &'a HistoryIndex,
) -> Option<&'a IndexedTopicIdentityObservation> {
    let operation_ids = scenario.steps.iter().filter_map(|step| match &step.action {
        ScenarioAction::DescribeTopics(action)
            if action.selection == TopicSelection::TopicId
                && action.topics.iter().any(|topic| {
                    topic.topic == record.record.topic
                        && topic
                            .expected_partitions
                            .as_ref()
                            .is_some_and(|partitions| partitions.contains(&record.record.partition))
                }) =>
        {
            Some(&action.operation_id)
        }
        _ => None,
    });
    one_source(operation_ids, &index.topic_identities_observed)
}

fn partition_offsets<'a>(
    scenario: &'a Scenario,
    record: &BrokerObservation,
    index: &'a HistoryIndex,
) -> Option<&'a IndexedPartitionOffsetsObservation> {
    let operation_ids = scenario.steps.iter().filter_map(|step| match &step.action {
        ScenarioAction::ListOffsetsBatch(action)
            if action.queries.iter().any(|query| {
                query.topic == record.record.topic
                    && query.partition == record.record.partition
                    && query.position == AdminOffsetPosition::Latest
            }) =>
        {
            Some(&action.operation_id)
        }
        _ => None,
    });
    one_source(operation_ids, &index.partition_offsets_observed)
}

fn one_source<'a, T>(
    mut operation_ids: impl Iterator<Item = &'a OperationId>,
    values: &'a BTreeMap<OperationId, Vec<T>>,
) -> Option<&'a T> {
    let operation_id = operation_ids.next()?;
    if operation_ids.next().is_some() {
        return None;
    }
    one(values.get(operation_id))
}

fn one<T>(values: Option<&Vec<T>>) -> Option<&T> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}

fn evidence(
    receive: Option<&crate::index::IndexedReceive>,
    record: Option<&&BrokerObservation>,
    identity: Option<&IndexedTopicIdentityObservation>,
    offsets: Option<&IndexedPartitionOffsetsObservation>,
) -> Vec<String> {
    receive
        .map(|value| format!("history:{}", value.history_sequence))
        .into_iter()
        .chain(record.map(|value| format!("broker-observation:{}", value.observation)))
        .chain(identity.map(|value| format!("broker-state-observation:{}", value.observation)))
        .chain(offsets.map(|value| format!("broker-state-observation:{}", value.observation)))
        .collect()
}
