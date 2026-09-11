//! Consumer-group batch deletion polls independent group state to complete absence.

use std::collections::{BTreeMap, BTreeSet};
use std::thread;
use std::time::Duration;

use testlab_schema::{BrokerConsumerGroupState, BrokerStateObservation};

use crate::observer_admin::{AdminObserverRequest, client};
use crate::observer_admin_group::{OwnedGroup, fetch, validate_group};
use crate::observer_admin_target::{ListTarget, ordinal};
use crate::observer_error::ObserverError;

const POLL_SLICE: Duration = Duration::from_millis(50);

pub(super) fn capture(
    request: AdminObserverRequest<'_>,
    target: &ListTarget,
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    let admin = client(request, "consumer-group-deletions")?;
    loop {
        let observations = normalize(
            request.first_observation,
            target,
            fetch(&admin, None, request.deadline)?,
        )?;
        if observations.iter().all(|observation| {
            matches!(
                observation,
                BrokerStateObservation::ConsumerGroup(group) if !group.exists
            )
        }) {
            return Ok(observations);
        }
        let wait = request
            .deadline
            .saturating_duration_since(std::time::Instant::now());
        if wait.is_zero() {
            return Err(ObserverError::Deadline);
        }
        thread::sleep(POLL_SLICE.min(wait));
    }
}

fn normalize(
    first_observation: u64,
    target: &ListTarget,
    groups: Vec<OwnedGroup>,
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    let requested = target
        .names
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut by_name = BTreeMap::new();
    for group in groups {
        if !requested.contains(group.name.as_str()) {
            continue;
        }
        validate_group(&group)?;
        let name = group.name.clone();
        if by_name.insert(name.clone(), group).is_some() {
            return Err(invalid(format!("returned duplicate group {name}")));
        }
    }
    target
        .names
        .iter()
        .enumerate()
        .map(|(index, group_id)| {
            let group = by_name.remove(group_id);
            Ok(BrokerStateObservation::ConsumerGroup(
                BrokerConsumerGroupState {
                    observation: ordinal(first_observation, index)?,
                    operation_id: target.operation_id.clone(),
                    group_id: group_id.clone(),
                    exists: group.is_some(),
                    member_count: group.map(|group| group.member_count),
                },
            ))
        })
        .collect()
}

fn invalid(detail: impl std::fmt::Display) -> ObserverError {
    ObserverError::InvalidBrokerState(format!("consumer-group deletion query {detail}"))
}

#[cfg(test)]
pub(super) fn normalize_fixture(
    first_observation: u64,
    target: &ListTarget,
    groups: Vec<(String, u32, &str, &str)>,
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    normalize(
        first_observation,
        target,
        groups
            .into_iter()
            .map(|(name, member_count, state, protocol_type)| OwnedGroup {
                name,
                member_count,
                state: state.to_owned(),
                protocol_type: protocol_type.to_owned(),
            })
            .collect(),
    )
}
