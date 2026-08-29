//! Group ownership verification binds stable public assignments to exact committed records.

use std::collections::{BTreeMap, BTreeSet};

use testlab_schema::{
    ConsumerId, GroupProtocol, ObserveGroupAssignmentsAction, Scenario, ScenarioAction,
    TopicPartitionIdentity, Violation,
};

use crate::consumer::{exact_record, sent_record};
use crate::index::{HistoryIndex, IndexedGroupAssignments};
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for step in &scenario.steps {
        if !index.action_issued(&step.action) {
            continue;
        }
        match &step.action {
            ScenarioAction::ObserveGroupAssignments(action) => {
                verify_assignment_observation(scenario, action, index, violations);
            }
            ScenarioAction::GroupReceiveSet(action) => {
                verify_receive_set(scenario, action, index, violations);
            }
            _ => {}
        }
    }
    crate::group_redistribution::verify(scenario, index, violations);
}

fn verify_assignment_observation(
    scenario: &Scenario,
    action: &ObserveGroupAssignmentsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let values = index.group_assignments.get(&action.operation_id);
    if values.map_or(0, Vec::len) != 1 {
        violations.push(violation(
            "CONS-005",
            format!(
                "assignment observation {} expected one completion, observed {}",
                action.operation_id,
                values.map_or(0, Vec::len)
            ),
            Some(action.operation_id.clone()),
            assignment_references(values.map(Vec::as_slice)),
        ));
        return;
    }
    let indexed = &values.map_or(&[][..], Vec::as_slice)[0];
    let actual = &indexed.observation.assignments;
    let ordered = actual.len() == action.consumer_ids.len()
        && actual
            .iter()
            .zip(&action.consumer_ids)
            .all(|(assignment, expected)| &assignment.consumer_id == expected);
    let mut union = BTreeSet::new();
    let mut total = 0;
    let mut members = BTreeSet::new();
    let mut metadata_valid = ordered;
    for assignment in actual {
        total += assignment.partitions.len();
        union.extend(assignment.partitions.iter().cloned());
        metadata_valid &= !assignment.member_id.is_empty()
            && members.insert(assignment.member_id.clone())
            && assignment.assignment_epoch > 0
            && assignment.group_epoch.is_positive()
            && expected_group(scenario, &assignment.consumer_id).is_some_and(
                |(group_id, protocol)| {
                    assignment.group_id == group_id && assignment.group_epoch.protocol() == protocol
                },
            );
    }
    let expected = action.partitions.iter().cloned().collect::<BTreeSet<_>>();
    if union != expected || total != union.len() {
        violations.push(violation(
            "CONS-006",
            format!(
                "assignment observation {} was not a complete pairwise-disjoint partition ownership set",
                action.operation_id
            ),
            Some(action.operation_id.clone()),
            vec![format!("history:{}", indexed.history_sequence)],
        ));
    }
    if !metadata_valid {
        violations.push(violation(
            "CONS-007",
            format!(
                "assignment observation {} did not preserve caller member order and positive matching public fences",
                action.operation_id
            ),
            Some(action.operation_id.clone()),
            vec![format!("history:{}", indexed.history_sequence)],
        ));
    }
    let declared = action.consumer_ids.iter().collect::<BTreeSet<_>>();
    if indexed.observation.transitions.iter().any(|transition| {
        !declared.contains(&transition.consumer_id)
            || transition.assignment_epoch == 0
            || !unique_partitions(&transition.partitions)
    }) {
        violations.push(violation(
            "CONS-007",
            format!(
                "assignment observation {} exposed an invalid public transition",
                action.operation_id
            ),
            Some(action.operation_id.clone()),
            vec![format!("history:{}", indexed.history_sequence)],
        ));
    }
}

fn verify_receive_set(
    scenario: &Scenario,
    action: &testlab_schema::GroupReceiveSetAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let values = index.group_receive_sets.get(&action.receive_id);
    if values.map_or(0, Vec::len) != 1 {
        violations.push(violation(
            "CONS-008",
            format!(
                "group receive set {} expected one completion, observed {}",
                action.receive_id,
                values.map_or(0, Vec::len)
            ),
            Some(action.receive_id.clone()),
            values
                .into_iter()
                .flatten()
                .map(|value| format!("history:{}", value.history_sequence))
                .collect(),
        ));
        return;
    }
    let indexed = &values.map_or(&[][..], Vec::as_slice)[0];
    let members = &indexed.completion.members;
    let ordered = members.len() == action.consumer_ids.len()
        && members
            .iter()
            .zip(&action.consumer_ids)
            .all(|(member, expected)| {
                &member.consumer_id == expected
                    && member.committed
                    && expected_group(scenario, expected).is_some_and(|(_, protocol)| {
                        member.group_epoch.is_some_and(|epoch| {
                            epoch.is_positive() && epoch.protocol() == protocol
                        })
                    })
            });
    let records = members
        .iter()
        .flat_map(|member| member.records.iter())
        .collect::<Vec<_>>();
    let exact = records.len() == action.expected_operation_ids.len()
        && action.expected_operation_ids.iter().all(|operation_id| {
            sent_record(scenario, operation_id).is_some_and(|expected| {
                records
                    .iter()
                    .filter(|record| exact_record(record, expected))
                    .count()
                    == 1
            })
        });
    if !ordered || !exact {
        violations.push(violation(
            "CONS-008",
            format!(
                "group receive set {} did not return and commit its exact expected public record set",
                action.receive_id
            ),
            Some(action.receive_id.clone()),
            vec![format!("history:{}", indexed.history_sequence)],
        ));
    }
    verify_receive_ownership(action, index, indexed, violations);
}

fn verify_receive_ownership(
    action: &testlab_schema::GroupReceiveSetAction,
    index: &HistoryIndex,
    receive: &crate::index::IndexedGroupReceiveSet,
    violations: &mut Vec<Violation>,
) {
    let assignment = index
        .group_assignments
        .values()
        .flatten()
        .filter(|value| value.history_sequence < receive.history_sequence)
        .filter(|value| {
            value
                .observation
                .assignments
                .iter()
                .map(|assignment| &assignment.consumer_id)
                .eq(action.consumer_ids.iter())
        })
        .max_by_key(|value| value.history_sequence);
    let owned = assignment.map(|value| {
        value
            .observation
            .assignments
            .iter()
            .map(|assignment| {
                (
                    &assignment.consumer_id,
                    assignment.partitions.iter().collect::<BTreeSet<_>>(),
                )
            })
            .collect::<BTreeMap<_, _>>()
    });
    let matches = owned.is_some_and(|owned| {
        receive.completion.members.iter().all(|member| {
            owned.get(&member.consumer_id).is_some_and(|partitions| {
                member.records.iter().all(|record| {
                    partitions.iter().any(|partition| {
                        partition.topic == record.topic && partition.partition == record.partition
                    })
                })
            })
        })
    });
    if !matches {
        violations.push(violation(
            "CONS-009",
            format!(
                "group receive set {} records did not follow the latest stable disjoint assignment",
                action.receive_id
            ),
            Some(action.receive_id.clone()),
            vec![format!("history:{}", receive.history_sequence)],
        ));
    }
}

fn expected_group<'a>(
    scenario: &'a Scenario,
    consumer_id: &ConsumerId,
) -> Option<(&'a str, GroupProtocol)> {
    scenario.steps.iter().find_map(|step| match &step.action {
        ScenarioAction::CreateGroupConsumer {
            consumer_id: created,
            group_id,
            protocol,
            ..
        } if created == consumer_id => Some((group_id.as_str(), *protocol)),
        _ => None,
    })
}

fn unique_partitions(partitions: &[TopicPartitionIdentity]) -> bool {
    partitions.iter().collect::<BTreeSet<_>>().len() == partitions.len()
}

fn assignment_references(values: Option<&[IndexedGroupAssignments]>) -> Vec<String> {
    values
        .into_iter()
        .flatten()
        .map(|value| format!("history:{}", value.history_sequence))
        .collect()
}
