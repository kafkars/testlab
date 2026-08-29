//! Consumer-group admin verification joins public claims to immediate broker-state queries.

use testlab_schema::{ScenarioAction, Violation};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::admin_group_evidence::{
    group_delete_evidence, group_description_evidence, group_list_evidence, offset_evidence,
};
use crate::admin_group_mutation::verify_group_offset_mutation;
use crate::index::{
    HistoryIndex, IndexedAdminGroupCompletion, IndexedConsumerGroupDescription,
    IndexedConsumerGroupObservation, IndexedConsumerGroupOffset,
    IndexedConsumerGroupOffsetObservation, IndexedConsumerGroupsList,
};
use crate::support::violation;

pub(crate) fn verify_group_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let command_window = index.admin_command_window(action);
    match action {
        ScenarioAction::ListConsumerGroupOffsets(expected) => verify_list_offset(
            expected,
            index
                .consumer_group_offsets_listed
                .get(&expected.operation_id),
            index
                .consumer_group_offsets_observed
                .get(&expected.operation_id),
            command_window,
            violations,
        ),
        ScenarioAction::ListConsumerGroups(expected) => verify_list_groups(
            expected,
            index.consumer_groups_listed.get(&expected.operation_id),
            index.consumer_groups_observed.get(&expected.operation_id),
            command_window,
            violations,
        ),
        ScenarioAction::DescribeConsumerGroup(expected) => verify_describe_group(
            expected,
            index.consumer_groups_described.get(&expected.operation_id),
            index.consumer_groups_observed.get(&expected.operation_id),
            command_window,
            violations,
        ),
        ScenarioAction::AlterConsumerGroupOffset(_)
        | ScenarioAction::DeleteConsumerGroupOffset(_) => {
            verify_group_offset_mutation(action, index, violations);
        }
        ScenarioAction::DeleteConsumerGroup(expected) => verify_delete_group(
            expected,
            index.consumer_groups_deleted.get(&expected.operation_id),
            index.consumer_groups_observed.get(&expected.operation_id),
            command_window,
            violations,
        ),
        _ => return false,
    }
    true
}

fn verify_list_offset(
    expected: &testlab_schema::ListConsumerGroupOffsetsAction,
    public: Option<&Vec<IndexedConsumerGroupOffset>>,
    independent: Option<&Vec<IndexedConsumerGroupOffsetObservation>>,
    command_window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let matches = |group: &str, topic: &str, partition: i32, offset: Option<i64>| {
        group == expected.group_id
            && topic == expected.topic
            && partition == expected.partition
            && offset == Some(expected.expected_offset)
    };
    let public_value = public
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let public_matches = public_value.is_some_and(|value| {
        matches(&value.group_id, &value.topic, value.partition, value.offset)
            && public_after_command(command_window, value.history_sequence)
    });
    let independent_matches = independent.is_some_and(|values| {
        values.len() == 1
            && values.first().is_some_and(|value| {
                matches(&value.group_id, &value.topic, value.partition, value.offset)
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
        "ADMIN-006",
        format!(
            "admin operation {} expected public and independent offset {} for group {} at {}[{}]",
            expected.operation_id,
            expected.expected_offset,
            expected.group_id,
            expected.topic,
            expected.partition
        ),
        Some(expected.operation_id.clone()),
        offset_evidence(public, independent),
    ));
}

fn verify_list_groups(
    expected: &testlab_schema::ListConsumerGroupsAction,
    public: Option<&Vec<IndexedConsumerGroupsList>>,
    independent: Option<&Vec<IndexedConsumerGroupObservation>>,
    command_window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let public_value = public
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let public_matches = public_value.is_some_and(|value| {
        value.broker_errors.is_empty()
            && strictly_sorted(&value.group_ids)
            && expected
                .required_group_ids
                .iter()
                .all(|group| value.group_ids.binary_search(group).is_ok())
            && public_after_command(command_window, value.history_sequence)
    });
    let independent_matches = independent.is_some_and(|values| {
        values.len() == expected.required_group_ids.len()
            && expected.required_group_ids.iter().all(|group| {
                values
                    .iter()
                    .any(|value| value.group_id == *group && value.exists)
            })
            && public_value.is_some_and(|public| {
                values.iter().all(|value| {
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
        "ADMIN-009",
        format!("admin operation {} expected a complete sorted listing containing independently present groups {:?}", expected.operation_id, expected.required_group_ids),
        Some(expected.operation_id.clone()),
        group_list_evidence(public, independent),
    ));
}

fn verify_describe_group(
    expected: &testlab_schema::DescribeConsumerGroupAction,
    public: Option<&Vec<IndexedConsumerGroupDescription>>,
    independent: Option<&Vec<IndexedConsumerGroupObservation>>,
    command_window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let public_value = public
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let public_matches = public_value.is_some_and(|value| {
        value.group_id == expected.group_id
            && value.member_count == expected.expected_member_count
            && public_after_command(command_window, value.history_sequence)
    });
    let independent_matches = independent.is_some_and(|values| {
        values.len() == 1
            && values.first().is_some_and(|value| {
                value.group_id == expected.group_id
                    && value.exists
                    && value.member_count == Some(expected.expected_member_count)
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
        "ADMIN-010",
        format!("admin operation {} expected group {} with {} member(s) in both public and independent descriptions", expected.operation_id, expected.group_id, expected.expected_member_count),
        Some(expected.operation_id.clone()),
        group_description_evidence(public, independent),
    ));
}

fn verify_delete_group(
    expected: &testlab_schema::DeleteConsumerGroupAction,
    public: Option<&Vec<IndexedAdminGroupCompletion>>,
    independent: Option<&Vec<IndexedConsumerGroupObservation>>,
    command_window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let public_value = public
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let public_matches = public_value.is_some_and(|value| {
        value.group_id == expected.group_id
            && public_after_command(command_window, value.history_sequence)
    });
    let independent_matches = independent.is_some_and(|values| {
        values.len() == 1
            && values.first().is_some_and(|value| {
                value.group_id == expected.group_id
                    && !value.exists
                    && value.member_count.is_none()
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
        "ADMIN-013",
        format!(
            "admin operation {} expected one deletion of group {} and independent absence",
            expected.operation_id, expected.group_id
        ),
        Some(expected.operation_id.clone()),
        group_delete_evidence(public, independent),
    ));
}

fn strictly_sorted(values: &[String]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}
