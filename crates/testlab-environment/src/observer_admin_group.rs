//! Consumer-group observation retains only exact names, existence, and member counts.

use std::collections::BTreeMap;
use std::thread;
use std::time::Duration;

use testlab_schema::{BrokerConsumerGroupState, BrokerStateObservation};

use crate::observer::remaining;
use crate::observer_admin::{AdminObserverRequest, client};
use crate::observer_admin_target::{GroupTarget, ListTarget, ordinal};
use crate::observer_error::ObserverError;

const POLL_SLICE: Duration = Duration::from_millis(50);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct OwnedGroup {
    pub(super) name: String,
    pub(super) member_count: u32,
    pub(super) state: String,
    pub(super) protocol_type: String,
}

pub(super) fn capture_groups(
    request: AdminObserverRequest<'_>,
    target: &ListTarget,
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    let admin = client(request, "consumer-groups")?;
    let groups = fetch(&admin, None, request.deadline)?;
    let by_name = index(groups)?;
    target
        .names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let observed = by_name.get(name);
            Ok(BrokerStateObservation::ConsumerGroup(
                BrokerConsumerGroupState {
                    observation: ordinal(request.first_observation, index)?,
                    operation_id: target.operation_id.clone(),
                    group_id: name.clone(),
                    exists: observed.is_some(),
                    member_count: observed.map(|group| group.member_count),
                },
            ))
        })
        .collect()
}

pub(super) fn capture_group(
    request: AdminObserverRequest<'_>,
    target: &GroupTarget,
) -> Result<BrokerStateObservation, ObserverError> {
    let admin = client(request, "consumer-group")?;
    loop {
        let groups = fetch(&admin, Some(&target.group_id), request.deadline)?;
        let observed = normalize_exact(request.first_observation, target, groups)?;
        if !target.poll_expected || group_matches(&observed, target) {
            return Ok(observed);
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

pub(super) fn fetch(
    admin: &rdkafka::admin::AdminClient<rdkafka::client::DefaultClientContext>,
    group_id: Option<&str>,
    deadline: std::time::Instant,
) -> Result<Vec<OwnedGroup>, ObserverError> {
    let groups = admin
        .inner()
        .fetch_group_list(group_id, remaining(deadline)?)?;
    groups
        .groups()
        .iter()
        .map(|group| {
            Ok(OwnedGroup {
                name: group.name().to_owned(),
                member_count: u32::try_from(group.members().len()).map_err(|_| {
                    ObserverError::InvalidBrokerState(format!(
                        "consumer group {} member count overflowed",
                        group.name()
                    ))
                })?,
                state: group.state().to_owned(),
                protocol_type: group.protocol_type().to_owned(),
            })
        })
        .collect()
}

pub(super) fn validate_group(group: &OwnedGroup) -> Result<(), ObserverError> {
    const KNOWN_STATES: [&str; 5] = [
        "PreparingRebalance",
        "CompletingRebalance",
        "Stable",
        "Dead",
        "Empty",
    ];
    if !KNOWN_STATES.contains(&group.state.as_str()) || group.protocol_type != "consumer" {
        return Err(ObserverError::InvalidBrokerState(format!(
            "consumer group {} returned non-authoritative state {:?} with protocol type {:?}",
            group.name, group.state, group.protocol_type
        )));
    }
    Ok(())
}

fn index(groups: Vec<OwnedGroup>) -> Result<BTreeMap<String, OwnedGroup>, ObserverError> {
    let mut indexed = BTreeMap::new();
    for group in groups {
        validate_group(&group)?;
        let name = group.name.clone();
        if indexed.insert(name.clone(), group).is_some() {
            return Err(ObserverError::InvalidBrokerState(format!(
                "consumer group listing repeated {name}"
            )));
        }
    }
    Ok(indexed)
}

fn normalize_exact(
    observation: u64,
    target: &GroupTarget,
    groups: Vec<OwnedGroup>,
) -> Result<BrokerStateObservation, ObserverError> {
    let mut groups = groups.into_iter();
    let Some(group) = groups.next() else {
        return Ok(BrokerStateObservation::ConsumerGroup(
            BrokerConsumerGroupState {
                observation,
                operation_id: target.operation_id.clone(),
                group_id: target.group_id.clone(),
                exists: false,
                member_count: None,
            },
        ));
    };
    if group.name != target.group_id || groups.next().is_some() {
        return Err(ObserverError::InvalidBrokerState(format!(
            "consumer group query for {} returned a duplicate or mismatched group",
            target.group_id
        )));
    }
    validate_group(&group)?;
    Ok(BrokerStateObservation::ConsumerGroup(
        BrokerConsumerGroupState {
            observation,
            operation_id: target.operation_id.clone(),
            group_id: group.name,
            exists: true,
            member_count: Some(group.member_count),
        },
    ))
}

fn group_matches(observation: &BrokerStateObservation, target: &GroupTarget) -> bool {
    let BrokerStateObservation::ConsumerGroup(observed) = observation else {
        return false;
    };
    observed.exists == target.expected_exists
        && target
            .expected_member_count
            .is_none_or(|count| observed.member_count == Some(count))
}

#[cfg(test)]
pub(super) fn normalize_fixture(
    observation: u64,
    target: &GroupTarget,
    groups: Vec<(String, u32)>,
) -> Result<BrokerStateObservation, ObserverError> {
    normalize_fixture_with_state(
        observation,
        target,
        groups
            .into_iter()
            .map(|(name, member_count)| (name, member_count, "Stable", "consumer"))
            .collect(),
    )
}

#[cfg(test)]
pub(super) fn normalize_fixture_with_state(
    observation: u64,
    target: &GroupTarget,
    groups: Vec<(String, u32, &str, &str)>,
) -> Result<BrokerStateObservation, ObserverError> {
    let groups = groups
        .into_iter()
        .map(|(name, member_count, state, protocol_type)| OwnedGroup {
            name,
            member_count,
            state: state.to_owned(),
            protocol_type: protocol_type.to_owned(),
        })
        .collect();
    normalize_exact(observation, target, groups)
}
