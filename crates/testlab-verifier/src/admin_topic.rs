//! Topic mutation verification requires one public result and one immediate metadata snapshot.

use testlab_schema::{CreateTopicAction, OperationId, Scenario, ScenarioAction, Violation};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::{HistoryIndex, IndexedAdminTopicCompletion, IndexedTopicObservation};
use crate::support::violation;

#[cfg(test)]
#[path = "admin_topic_creation_config_test.rs"]
mod creation_config_test;
#[cfg(test)]
#[path = "admin_topic_manual_placement_test.rs"]
mod manual_placement_test;
#[path = "admin_partition_manual_placement.rs"]
mod partition_manual_placement;
#[path = "admin_topic_creation_config.rs"]
mod topic_creation_config;
#[path = "admin_topic_topology.rs"]
pub(crate) mod topology;

pub(crate) fn contract(action: &CreateTopicAction) -> &'static str {
    if action.expected_error_code.is_some() {
        "ADMIN-014"
    } else if action.validate_only {
        "ADMIN-020"
    } else if action.replica_assignments.is_some() {
        "ADMIN-084"
    } else if !action.configs.is_empty() {
        "ADMIN-094"
    } else {
        "ADMIN-001"
    }
}

pub(crate) fn verify_topic_action(
    scenario: &Scenario,
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let command_window = index.admin_command_window(action);
    match action {
        ScenarioAction::CreateTopic(action)
            if action.expected_error_code.is_none() && !action.validate_only =>
        {
            if action.replica_assignments.is_some() {
                verify_manual_topic_creation(action, index, command_window, violations);
            } else {
                verify(
                    contract(action),
                    "topic creation",
                    &action.operation_id,
                    &action.topic,
                    Some((0..action.partitions).collect()),
                    index.topics_created.get(&action.operation_id),
                    index.topics_observed.get(&action.operation_id),
                    command_window,
                    violations,
                );
                if !action.configs.is_empty() {
                    topic_creation_config::verify(scenario, action, index, violations);
                }
            }
        }
        ScenarioAction::CreatePartitions(action)
            if action.expected_error_code.is_none() && !action.validate_only =>
        {
            if action.replica_assignments.is_some() {
                partition_manual_placement::verify(action, index, command_window, violations);
            } else {
                verify(
                    "ADMIN-002",
                    "partition creation",
                    &action.operation_id,
                    &action.topic,
                    Some((0..action.total_count).collect()),
                    index.topic_partitions_created.get(&action.operation_id),
                    index.topics_observed.get(&action.operation_id),
                    command_window,
                    violations,
                );
            }
        }
        ScenarioAction::DeleteTopic(action) if action.expected_error_code.is_none() => verify(
            "ADMIN-007",
            "topic deletion",
            &action.operation_id,
            &action.topic,
            None,
            index.topics_deleted.get(&action.operation_id),
            index.topics_observed.get(&action.operation_id),
            command_window,
            violations,
        ),
        _ => return false,
    }
    true
}

fn verify_manual_topic_creation(
    action: &CreateTopicAction,
    index: &HistoryIndex,
    command_window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let public = index
        .topics_created
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
    let matches = public.is_some_and(|public| {
        public.topic == action.topic
            && public_after_command(command_window, public.history_sequence)
            && independent.is_some_and(|independent| {
                independent.value.operation_id == action.operation_id
                    && complete_topic_matches(action, &independent.value.topic_partitions)
                    && independent.value.assignments.len() == expected.len()
                    && independent.value.assignments.iter().zip(expected).all(
                        |(actual, expected)| {
                            let mut expected_isr = expected.broker_ids.clone();
                            expected_isr.sort_unstable();
                            actual.topic == action.topic
                                && actual.partition == expected.partition_index
                                && actual.replicas == expected.broker_ids
                                && actual.in_sync_replicas == expected_isr
                                && actual.replicas.contains(&actual.leader_id)
                        },
                    )
                    && immediate_after_public(
                        command_window,
                        public.history_sequence,
                        independent.history_sequence,
                    )
            })
    });
    if !matches {
        violations.push(violation(
            "ADMIN-084",
            format!(
                "admin operation {} expected one exact manually placed topic creation and immediate caller-ordered replica assignments with full ISR",
                action.operation_id
            ),
            Some(action.operation_id.clone()),
            public
                .map(|value| format!("history:{}", value.history_sequence))
                .into_iter()
                .chain(independent.map(|value| {
                    format!(
                        "broker-state-observation:{}",
                        value.value.observation
                    )
                }))
                .collect(),
        ));
    }
}

fn complete_topic_matches(
    action: &CreateTopicAction,
    topics: &[testlab_schema::BrokerTopicPartitionSet],
) -> bool {
    let [topic] = topics else {
        return false;
    };
    topic.topic == action.topic && topic.partitions == (0..action.partitions).collect::<Vec<_>>()
}

#[allow(
    clippy::too_many_arguments,
    clippy::needless_pass_by_value,
    reason = "the verifier joins explicit fields and owns its derived expected partition vector"
)]
fn verify(
    contract: &str,
    operation: &str,
    operation_id: &OperationId,
    topic: &str,
    expected_partitions: Option<Vec<i32>>,
    public: Option<&Vec<IndexedAdminTopicCompletion>>,
    independent: Option<&Vec<IndexedTopicObservation>>,
    command_window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let public_value = public
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let public_matches = public_value.is_some_and(|value| {
        value.topic == topic && public_after_command(command_window, value.history_sequence)
    });
    let independent_matches = independent.is_some_and(|values| {
        values.len() == 1
            && values.first().is_some_and(|value| {
                value.topic == topic
                    && public_value.is_some_and(|public| {
                        immediate_after_public(
                            command_window,
                            public.history_sequence,
                            value.history_sequence,
                        )
                    })
                    && match &expected_partitions {
                        Some(partitions) => value.exists && &value.partitions == partitions,
                        None => !value.exists && value.partitions.is_empty(),
                    }
            })
    });
    if public_matches && independent_matches {
        return;
    }
    let expectation = expected_partitions.as_ref().map_or_else(
        || "absent".to_owned(),
        |value| format!("present with partitions {value:?}"),
    );
    violations.push(violation(
        contract,
        format!("admin operation {operation_id} expected one exact {operation} for {topic} and independent state {expectation}"),
        Some(operation_id.clone()),
        evidence(public, independent),
    ));
}

fn evidence(
    public: Option<&Vec<IndexedAdminTopicCompletion>>,
    independent: Option<&Vec<IndexedTopicObservation>>,
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
