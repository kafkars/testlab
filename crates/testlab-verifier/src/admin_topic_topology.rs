//! Topic-description topology checks exercise every public partition getter.

use std::collections::BTreeSet;

use testlab_schema::{
    AdminTopicDescriptionPage, AdminTopicDescriptionValue, AdminTopicPartitionDescriptionOutcome,
    OperationId, TopicDescriptionApi, Violation,
};

use crate::support::violation;

#[cfg(test)]
#[path = "admin_topic_topology_test.rs"]
mod test;

pub(crate) fn verify_single(
    operation_id: &OperationId,
    api: TopicDescriptionApi,
    partitions: &[i32],
    details: &[AdminTopicPartitionDescriptionOutcome],
    pages: &[AdminTopicDescriptionPage],
    history_sequence: u64,
    violations: &mut Vec<Violation>,
) {
    let valid = match api {
        TopicDescriptionApi::Metadata => {
            pages.is_empty() && details_match(partitions, details, false)
        }
        TopicDescriptionApi::DescribeTopicPartitions => {
            details_match(partitions, details, true) && page_details_match(details, pages)
        }
    };
    if !valid {
        push(operation_id, history_sequence, violations);
    }
}

pub(crate) fn verify_values<'a>(
    operation_id: &OperationId,
    values: impl Iterator<Item = &'a AdminTopicDescriptionValue>,
    history_sequence: u64,
    violations: &mut Vec<Violation>,
) {
    if values
        .into_iter()
        .any(|value| !metadata_details_valid(&value.partitions))
    {
        push(operation_id, history_sequence, violations);
    }
}

fn metadata_details_valid(details: &[AdminTopicPartitionDescriptionOutcome]) -> bool {
    let partitions = details
        .iter()
        .map(|partition| partition.partition)
        .collect::<Vec<_>>();
    details_match(&partitions, details, false)
}

fn page_details_match(
    aggregate: &[AdminTopicPartitionDescriptionOutcome],
    pages: &[AdminTopicDescriptionPage],
) -> bool {
    pages
        .iter()
        .all(|page| details_match(&page.partitions, &page.partition_details, true))
        && pages
            .iter()
            .flat_map(|page| page.partition_details.iter())
            .eq(aggregate.iter())
}

fn details_match(
    partitions: &[i32],
    details: &[AdminTopicPartitionDescriptionOutcome],
    allows_eligible_leaders: bool,
) -> bool {
    partitions.len() == details.len()
        && partitions
            .iter()
            .copied()
            .eq(details.iter().map(|detail| detail.partition))
        && details
            .windows(2)
            .all(|pair| pair[0].partition < pair[1].partition)
        && details
            .iter()
            .all(|detail| valid_detail(detail, allows_eligible_leaders))
}

fn valid_detail(
    detail: &AdminTopicPartitionDescriptionOutcome,
    allows_eligible_leaders: bool,
) -> bool {
    let Some(leader_id) = detail.leader_id else {
        return false;
    };
    let Some(leader_epoch) = detail.leader_epoch else {
        return false;
    };
    detail.partition >= 0
        && detail.error_code.is_none()
        && leader_id >= 0
        && leader_epoch >= 0
        && nonempty_unique_nonnegative(&detail.replicas)
        && unique_nonnegative(&detail.in_sync_replicas)
        && detail.in_sync_replicas.contains(&leader_id)
        && subset(&detail.in_sync_replicas, &detail.replicas)
        && unique_nonnegative(&detail.offline_replicas)
        && subset(&detail.offline_replicas, &detail.replicas)
        && disjoint(&detail.in_sync_replicas, &detail.offline_replicas)
        && optional_replicas_valid(detail.eligible_leader_replicas.as_deref())
        && optional_replicas_valid(detail.last_known_eligible_leader_replicas.as_deref())
        && (allows_eligible_leaders
            || (detail.eligible_leader_replicas.is_none()
                && detail.last_known_eligible_leader_replicas.is_none()))
}

fn nonempty_unique_nonnegative(values: &[i32]) -> bool {
    !values.is_empty() && unique_nonnegative(values)
}

fn optional_replicas_valid(values: Option<&[i32]>) -> bool {
    values.is_none_or(unique_nonnegative)
}

fn unique_nonnegative(values: &[i32]) -> bool {
    values.iter().all(|value| *value >= 0)
        && values.iter().copied().collect::<BTreeSet<_>>().len() == values.len()
}

fn subset(values: &[i32], replicas: &[i32]) -> bool {
    values.iter().all(|value| replicas.contains(value))
}

fn disjoint(left: &[i32], right: &[i32]) -> bool {
    left.iter().all(|value| !right.contains(value))
}

fn push(operation_id: &OperationId, history_sequence: u64, violations: &mut Vec<Violation>) {
    violations.push(violation(
        "ADMIN-097",
        format!(
            "admin operation {operation_id} expected complete internally consistent public partition topology"
        ),
        Some(operation_id.clone()),
        vec![format!("history:{history_sequence}")],
    ));
}

#[cfg(test)]
pub(crate) fn partition(partition: i32) -> AdminTopicPartitionDescriptionOutcome {
    AdminTopicPartitionDescriptionOutcome {
        partition,
        error_code: None,
        leader_id: Some(1),
        leader_epoch: Some(1),
        replicas: vec![1],
        in_sync_replicas: vec![1],
        eligible_leader_replicas: None,
        last_known_eligible_leader_replicas: None,
        offline_replicas: Vec::new(),
    }
}

#[cfg(test)]
pub(crate) fn partitions(values: &[i32]) -> Vec<AdminTopicPartitionDescriptionOutcome> {
    values.iter().copied().map(partition).collect()
}
