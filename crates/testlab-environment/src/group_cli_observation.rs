//! Pinned Kafka CLI state rows provide exact classic and modern group snapshots.

use std::collections::BTreeMap;

use testlab_schema::{BrokerConsumerGroupState, BrokerStateObservation};

use crate::observer_admin_target::{AdminTarget, ordinal};
use crate::observer_error::ObserverError;

pub(super) fn supports(target: &AdminTarget) -> bool {
    match target {
        AdminTarget::ConsumerGroup(target) => !target.poll_expected,
        AdminTarget::ConsumerGroups(_) | AdminTarget::ClassicGroups(_) => true,
        _ => false,
    }
}

pub(super) fn selection(target: &AdminTarget) -> Vec<String> {
    match target {
        AdminTarget::ConsumerGroups(_) => vec!["--all-groups".to_owned()],
        AdminTarget::ConsumerGroup(target) => vec!["--group".to_owned(), target.group_id.clone()],
        AdminTarget::ClassicGroups(target) => target
            .group_ids
            .iter()
            .flat_map(|group| ["--group".to_owned(), group.clone()])
            .collect(),
        _ => Vec::new(),
    }
}

pub(super) fn normalize(
    first: u64,
    target: &AdminTarget,
    stdout: &[u8],
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    let groups = parse(stdout)?;
    let (names, exact) = match target {
        AdminTarget::ConsumerGroups(target) => (target.names.as_slice(), false),
        AdminTarget::ConsumerGroup(target) => (std::slice::from_ref(&target.group_id), true),
        AdminTarget::ClassicGroups(target) => (target.group_ids.as_slice(), true),
        _ => return Err(invalid("unsupported group snapshot target")),
    };
    if exact && (groups.len() != names.len() || names.iter().any(|name| !groups.contains_key(name)))
    {
        return Err(invalid("description omitted or added a requested group"));
    }
    names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let count = groups.get(name).copied();
            Ok(BrokerStateObservation::ConsumerGroup(
                BrokerConsumerGroupState {
                    observation: ordinal(first, index)?,
                    operation_id: target.operation_id().clone(),
                    group_id: name.clone(),
                    exists: count.is_some(),
                    member_count: count,
                },
            ))
        })
        .collect()
}

fn parse(stdout: &[u8]) -> Result<BTreeMap<String, u32>, ObserverError> {
    let text = std::str::from_utf8(stdout).map_err(|_| invalid("state output is not UTF-8"))?;
    let mut groups = BTreeMap::new();
    let mut header_pending = false;
    for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields
            == [
                "GROUP",
                "COORDINATOR",
                "(ID)",
                "ASSIGNMENT-STRATEGY",
                "STATE",
                "#MEMBERS",
            ]
        {
            if header_pending {
                return Err(invalid("state header has no row"));
            }
            header_pending = true;
            continue;
        }
        let [group, coordinator, node, _assignor, state, members] = fields.as_slice() else {
            return Err(invalid("unexpected state row shape"));
        };
        if !header_pending
            || !coordinator.contains(':')
            || node
                .strip_prefix('(')
                .and_then(|node| node.strip_suffix(')'))
                .and_then(|node| node.parse::<u32>().ok())
                .is_none()
            || !matches!(
                *state,
                "Stable"
                    | "Empty"
                    | "PreparingRebalance"
                    | "CompletingRebalance"
                    | "Assigning"
                    | "Reconciling"
            )
        {
            return Err(invalid("non-authoritative state row"));
        }
        let count = members
            .parse::<u32>()
            .map_err(|_| invalid("invalid member count"))?;
        if (*state == "Empty" && count != 0) || groups.insert((*group).to_owned(), count).is_some()
        {
            return Err(invalid("duplicate group or inconsistent empty state"));
        }
        header_pending = false;
    }
    if header_pending {
        return Err(invalid("state header has no row"));
    }
    Ok(groups)
}

fn invalid(message: &str) -> ObserverError {
    ObserverError::InvalidBrokerState(format!("Kafka CLI group snapshot: {message}"))
}
