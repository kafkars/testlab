//! Classic-group batch observation selects exact requested groups from one snapshot.

use std::collections::{BTreeMap, BTreeSet};

use testlab_schema::{BrokerConsumerGroupState, BrokerStateObservation};

use crate::kafka_role_wire;
use crate::observer::remaining;
use crate::observer_admin::{AdminObserverRequest, client};
use crate::observer_admin_group::{OwnedGroup, fetch, validate_group};
use crate::observer_admin_target::{GroupIdsTarget, ordinal};
use crate::observer_error::ObserverError;

pub(super) fn capture(
    request: AdminObserverRequest<'_>,
    target: &GroupIdsTarget,
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    let admin = client(request, "classic-groups")?;
    let groups = fetch(&admin, None, request.deadline)?;
    normalize(
        request.first_observation,
        &target.operation_id,
        &target.group_ids,
        groups,
        &BTreeMap::new(),
    )
}

pub(super) fn capture_consumer_groups(
    request: AdminObserverRequest<'_>,
    target: &GroupIdsTarget,
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    let admin = client(request, "consumer-group-descriptions")?;
    let groups = fetch(&admin, None, request.deadline)?;
    let modern_counts = modern_member_counts(request, &target.group_ids)?;
    normalize(
        request.first_observation,
        &target.operation_id,
        &target.group_ids,
        groups,
        &modern_counts,
    )
}

fn modern_member_counts(
    request: AdminObserverRequest<'_>,
    group_ids: &[String],
) -> Result<BTreeMap<String, u32>, ObserverError> {
    if request.cluster_size != 1 || !request.security.supports_plaintext_wire() {
        return Ok(BTreeMap::new());
    }
    let mut counts = BTreeMap::new();
    for group_id in group_ids {
        if let Some(count) = kafka_role_wire::consumer_group_member_count(
            request.endpoint,
            group_id,
            remaining(request.deadline)?,
        )
        .map_err(ObserverError::InvalidBrokerState)?
        {
            counts.insert(group_id.clone(), count);
        }
    }
    Ok(counts)
}

fn normalize(
    first_observation: u64,
    operation_id: &testlab_schema::OperationId,
    group_ids: &[String],
    groups: Vec<OwnedGroup>,
    modern_counts: &BTreeMap<String, u32>,
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    let requested = group_ids
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut by_name = BTreeMap::new();
    for group in groups {
        if !requested.contains(group.name.as_str()) {
            continue;
        }
        let name = group.name.clone();
        if by_name.insert(name.clone(), group).is_some() {
            return Err(invalid(format!("returned duplicate group {name}")));
        }
    }
    group_ids
        .iter()
        .enumerate()
        .map(|(index, group_id)| {
            let group = by_name
                .remove(group_id)
                .ok_or_else(|| invalid(format!("did not return requested group {group_id}")))?;
            validate_group(&group)?;
            Ok(BrokerStateObservation::ConsumerGroup(
                BrokerConsumerGroupState {
                    observation: ordinal(first_observation, index)?,
                    operation_id: operation_id.clone(),
                    group_id: group.name,
                    exists: true,
                    member_count: Some(
                        modern_counts
                            .get(group_id)
                            .copied()
                            .unwrap_or(group.member_count),
                    ),
                },
            ))
        })
        .collect()
}

fn invalid(detail: impl std::fmt::Display) -> ObserverError {
    ObserverError::InvalidBrokerState(format!("classic-group batch query {detail}"))
}

#[cfg(test)]
pub(super) fn normalize_fixture(
    first_observation: u64,
    target: &GroupIdsTarget,
    groups: Vec<(String, u32, &str, &str)>,
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    let groups = groups
        .into_iter()
        .map(|(name, member_count, state, protocol_type)| OwnedGroup {
            name,
            member_count,
            state: state.to_owned(),
            protocol_type: protocol_type.to_owned(),
        })
        .collect();
    normalize(
        first_observation,
        &target.operation_id,
        &target.group_ids,
        groups,
        &BTreeMap::new(),
    )
}

#[cfg(test)]
pub(super) fn normalize_consumer_fixture(
    first_observation: u64,
    target: &GroupIdsTarget,
    groups: Vec<(String, u32, &str, &str)>,
    modern_counts: &BTreeMap<String, u32>,
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    let groups = groups
        .into_iter()
        .map(|(name, member_count, state, protocol_type)| OwnedGroup {
            name,
            member_count,
            state: state.to_owned(),
            protocol_type: protocol_type.to_owned(),
        })
        .collect();
    normalize(
        first_observation,
        &target.operation_id,
        &target.group_ids,
        groups,
        modern_counts,
    )
}
