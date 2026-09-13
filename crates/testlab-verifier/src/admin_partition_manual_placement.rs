//! Manual partition expansion joins exact public intent to immediate metadata.

use testlab_schema::{CreatePartitionsAction, Violation};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::support::violation;

#[cfg(test)]
#[path = "admin_partition_manual_placement_test.rs"]
mod test;

pub(super) fn verify(
    action: &CreatePartitionsAction,
    index: &HistoryIndex,
    command_window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let public = index
        .topic_partitions_created
        .get(&action.operation_id)
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let independent = index
        .admin_partition_reassignments
        .assignments_observed
        .get(&action.operation_id)
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let expected = action.replica_assignments.as_deref().unwrap_or_default();
    let first_partition = action.expected_current_count.unwrap_or_default();
    let matches = public.is_some_and(|public| {
        public.topic == action.topic
            && public_after_command(command_window, public.history_sequence)
            && independent.is_some_and(|independent| {
                independent.value.operation_id == action.operation_id
                    && complete_topic_matches(action, &independent.value.topic_partitions)
                    && independent.value.assignments.len() == expected.len()
                    && independent
                        .value
                        .assignments
                        .iter()
                        .zip(expected)
                        .enumerate()
                        .all(|(offset, (actual, replicas))| {
                            let mut expected_isr = replicas.clone();
                            expected_isr.sort_unstable();
                            let expected_partition = i32::try_from(offset)
                                .ok()
                                .and_then(|offset| first_partition.checked_add(offset));
                            actual.topic == action.topic
                                && Some(actual.partition) == expected_partition
                                && actual.replicas == *replicas
                                && actual.in_sync_replicas == expected_isr
                                && actual.replicas.contains(&actual.leader_id)
                        })
                    && immediate_after_public(
                        command_window,
                        public.history_sequence,
                        independent.history_sequence,
                    )
            })
    });
    if !matches {
        violations.push(violation(
            "ADMIN-085",
            format!(
                "admin operation {} expected one exact manually placed partition expansion and immediate caller-ordered replica assignments with full ISR",
                action.operation_id
            ),
            Some(action.operation_id.clone()),
            public
                .map(|value| format!("history:{}", value.history_sequence))
                .into_iter()
                .chain(independent.map(|value| {
                    format!("broker-state-observation:{}", value.value.observation)
                }))
                .collect(),
        ));
    }
}

fn complete_topic_matches(
    action: &CreatePartitionsAction,
    topics: &[testlab_schema::BrokerTopicPartitionSet],
) -> bool {
    let [topic] = topics else {
        return false;
    };
    topic.topic == action.topic && topic.partitions == (0..action.total_count).collect::<Vec<_>>()
}
