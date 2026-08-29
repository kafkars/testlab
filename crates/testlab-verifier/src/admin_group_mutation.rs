//! Consumer-group offset mutation verification requires exact independent postconditions.

use testlab_schema::{OperationId, ScenarioAction, Violation};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::{
    HistoryIndex, IndexedAdminGroupOffsetCompletion, IndexedConsumerGroupOffsetObservation,
};
use crate::support::violation;

pub(crate) fn verify_group_offset_mutation(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    match action {
        ScenarioAction::AlterConsumerGroupOffset(expected) => verify_offset_mutation(
            "ADMIN-011",
            "alteration",
            &expected.operation_id,
            &expected.group_id,
            &expected.topic,
            expected.partition,
            Some(expected.offset),
            index
                .consumer_group_offsets_altered
                .get(&expected.operation_id),
            index
                .consumer_group_offsets_observed
                .get(&expected.operation_id),
            index.admin_command_window(action),
            violations,
        ),
        ScenarioAction::DeleteConsumerGroupOffset(expected) => verify_offset_mutation(
            "ADMIN-012",
            "deletion",
            &expected.operation_id,
            &expected.group_id,
            &expected.topic,
            expected.partition,
            None,
            index
                .consumer_group_offsets_deleted
                .get(&expected.operation_id),
            index
                .consumer_group_offsets_observed
                .get(&expected.operation_id),
            index.admin_command_window(action),
            violations,
        ),
        _ => {}
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the verifier keeps every exact group-offset identity and evidence source explicit"
)]
fn verify_offset_mutation(
    contract: &str,
    operation: &str,
    operation_id: &OperationId,
    group_id: &str,
    topic: &str,
    partition: i32,
    expected_offset: Option<i64>,
    public: Option<&Vec<IndexedAdminGroupOffsetCompletion>>,
    independent: Option<&Vec<IndexedConsumerGroupOffsetObservation>>,
    command_window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let identity = |group: &str, actual_topic: &str, actual_partition: i32| {
        group == group_id && actual_topic == topic && actual_partition == partition
    };
    let public_value = public
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let public_matches = public_value.is_some_and(|value| {
        identity(&value.group_id, &value.topic, value.partition)
            && public_after_command(command_window, value.history_sequence)
    });
    let independent_matches = independent.is_some_and(|values| {
        values.len() == 1
            && values.first().is_some_and(|value| {
                identity(&value.group_id, &value.topic, value.partition)
                    && value.offset == expected_offset
                    && public_value.is_some_and(|public| {
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
        contract,
        format!("admin operation {operation_id} expected one exact offset {operation} and independent state {expected_offset:?} for {group_id} at {topic}[{partition}]"),
        Some(operation_id.clone()),
        mutation_evidence(public, independent),
    ));
}

fn mutation_evidence(
    public: Option<&Vec<IndexedAdminGroupOffsetCompletion>>,
    independent: Option<&Vec<IndexedConsumerGroupOffsetObservation>>,
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
