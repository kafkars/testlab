//! Classic-group batch observation selects exact requested groups from one snapshot.

use std::collections::{BTreeMap, BTreeSet};

use testlab_schema::{BrokerConsumerGroupState, BrokerStateObservation};

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
    )
}

pub(super) fn capture_consumer_groups(
    request: AdminObserverRequest<'_>,
    target: &GroupIdsTarget,
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    let admin = client(request, "consumer-group-descriptions")?;
    let groups = fetch(&admin, None, request.deadline)?;
    normalize(
        request.first_observation,
        &target.operation_id,
        &target.group_ids,
        groups,
    )
}

fn normalize(
    first_observation: u64,
    operation_id: &testlab_schema::OperationId,
    group_ids: &[String],
    groups: Vec<OwnedGroup>,
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
                    member_count: Some(group.member_count),
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
    )
}
