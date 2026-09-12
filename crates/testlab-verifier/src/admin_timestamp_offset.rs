//! Timestamp offset verification joins public selection to exact broker record truth.

use testlab_schema::{BrokerObservation, Violation};

use super::OffsetExpectation;
use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::{IndexedOffsetList, IndexedPartitionOffsetsObservation};
use crate::support::violation;

pub(super) fn verify(
    expected: OffsetExpectation<'_>,
    public: Option<&Vec<IndexedOffsetList>>,
    state: Option<&Vec<IndexedPartitionOffsetsObservation>>,
    window: Option<AdminCommandWindow>,
    observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) {
    let completion = one(public);
    let watermark = one(state);
    let selected = observations
        .iter()
        .filter(|value| {
            same_partition(expected, value)
                && value.offset == expected.expected_offset
                && value.record.timestamp_millis == expected.timestamp_millis
        })
        .collect::<Vec<_>>();
    let timestamp = expected.timestamp_millis;
    let public_matches = completion.is_some_and(|value| {
        value.topic == expected.topic
            && value.partition == expected.partition
            && value.offset == Some(expected.expected_offset)
            && value.timestamp_millis == timestamp
            && public_after_command(window, value.history_sequence)
    });
    let watermark_matches = watermark.is_some_and(|value| {
        value.topic == expected.topic
            && value.partition == expected.partition
            && value.low_watermark >= 0
            && expected.expected_offset >= value.low_watermark
            && expected.expected_offset < value.high_watermark
            && completion.is_some_and(|public| {
                immediate_after_public(window, public.history_sequence, value.history_sequence)
            })
    });
    let earlier_exists = timestamp.is_some_and(|timestamp| {
        observations.iter().any(|value| {
            same_partition(expected, value)
                && value.offset < expected.expected_offset
                && value
                    .record
                    .timestamp_millis
                    .is_some_and(|value| value < timestamp)
        })
    });
    if public_matches && watermark_matches && selected.len() == 1 && earlier_exists {
        return;
    }
    let mut evidence = public
        .into_iter()
        .flatten()
        .map(|value| format!("history:{}", value.history_sequence))
        .chain(
            state
                .into_iter()
                .flatten()
                .map(|value| format!("broker-state-observation:{}", value.observation)),
        )
        .collect::<Vec<_>>();
    evidence.extend(
        observations
            .iter()
            .filter(|value| same_partition(expected, value))
            .map(|value| format!("broker-observation:{}", value.observation)),
    );
    violations.push(violation(
        "ADMIN-077",
        format!(
            "admin operation {} expected timestamp {:?} to select offset {} with the same returned timestamp for {}[{}]",
            expected.operation_id,
            timestamp,
            expected.expected_offset,
            expected.topic,
            expected.partition
        ),
        Some(expected.operation_id.clone()),
        evidence,
    ));
}

fn same_partition(expected: OffsetExpectation<'_>, value: &BrokerObservation) -> bool {
    value.record.topic == expected.topic && value.record.partition == expected.partition
}

fn one<T>(values: Option<&Vec<T>>) -> Option<&T> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}
