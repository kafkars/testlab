//! Pinned Kafka Share-group state output becomes one exact independent fact.

use testlab_schema::{BrokerShareGroupState, BrokerStateObservation};

use crate::observer_admin_target::AdminTarget;
use crate::observer_error::ObserverError;

pub(super) fn normalize(
    observation: u64,
    target: &AdminTarget,
    stdout: &[u8],
) -> Result<BrokerStateObservation, ObserverError> {
    let AdminTarget::ShareGroup(target) = target else {
        return Err(invalid("unsupported observation target"));
    };
    let text = std::str::from_utf8(stdout).map_err(|_| invalid("state output is not UTF-8"))?;
    let mut lines = text.lines().map(str::trim).filter(|line| !line.is_empty());
    let header = lines
        .next()
        .ok_or_else(|| invalid("state output omitted its header"))?;
    if header.split_whitespace().collect::<Vec<_>>()
        != ["GROUP", "COORDINATOR", "(ID)", "STATE", "#MEMBERS"]
    {
        return Err(invalid("unexpected state header"));
    }
    let row = lines
        .next()
        .ok_or_else(|| invalid("state output omitted its row"))?;
    if lines.next().is_some() {
        return Err(invalid("state output contained extra rows"));
    }
    let fields = row.split_whitespace().collect::<Vec<_>>();
    let [group_id, coordinator, node, state, members] = fields.as_slice() else {
        return Err(invalid("unexpected state row shape"));
    };
    if *group_id != target.group_id
        || !coordinator.contains(':')
        || node
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(')'))
            .and_then(|value| value.parse::<i32>().ok())
            .is_none()
        || !matches!(
            *state,
            "Stable" | "Empty" | "Dead" | "Assigning" | "Reconciling"
        )
    {
        return Err(invalid("non-authoritative state row"));
    }
    let member_count = members
        .parse::<u32>()
        .map_err(|_| invalid("invalid member count"))?;
    if (*state == "Empty" || *state == "Dead") && member_count != 0 {
        return Err(invalid("inactive state reported active members"));
    }
    Ok(BrokerStateObservation::ShareGroup(BrokerShareGroupState {
        observation,
        operation_id: target.operation_id.clone(),
        group_id: target.group_id.clone(),
        exists: *state != "Dead",
        state: Some((*state).to_owned()),
        member_count: Some(member_count),
    }))
}

fn invalid(message: &str) -> ObserverError {
    ObserverError::InvalidBrokerState(format!("Kafka CLI Share-group snapshot: {message}"))
}
