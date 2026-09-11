//! Pinned Kafka Share-group state and offset output become exact independent facts.

use testlab_schema::{BrokerShareGroupOffset, BrokerShareGroupState, BrokerStateObservation};

use crate::observer_admin_target::AdminTarget;
use crate::observer_error::ObserverError;

pub(super) fn normalize(
    observation: u64,
    target: &AdminTarget,
    stdout: &[u8],
) -> Result<BrokerStateObservation, ObserverError> {
    let text = std::str::from_utf8(stdout).map_err(|_| invalid("state output is not UTF-8"))?;
    match target {
        AdminTarget::ShareGroup(target) => normalize_state(observation, target, text),
        AdminTarget::ShareGroupOffset(target) => normalize_offset(observation, target, text),
        _ => Err(invalid("unsupported observation target")),
    }
}

fn normalize_state(
    observation: u64,
    target: &crate::observer_admin_target::ShareGroupTarget,
    text: &str,
) -> Result<BrokerStateObservation, ObserverError> {
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

fn normalize_offset(
    observation: u64,
    target: &crate::observer_admin_target::ShareGroupOffsetTarget,
    text: &str,
) -> Result<BrokerStateObservation, ObserverError> {
    let mut lines = text.lines().map(str::trim).filter(|line| !line.is_empty());
    let header = lines
        .next()
        .ok_or_else(|| invalid("offset output omitted its header"))?;
    if header.split_whitespace().collect::<Vec<_>>()
        != ["GROUP", "TOPIC", "PARTITION", "START-OFFSET", "LAG"]
    {
        return Err(invalid("unexpected offset header"));
    }
    let row = lines
        .next()
        .ok_or_else(|| invalid("offset output omitted its row"))?;
    if lines.next().is_some() {
        return Err(invalid("offset output contained extra rows"));
    }
    let fields = row.split_whitespace().collect::<Vec<_>>();
    let [group_id, topic, partition, start_offset, lag] = fields.as_slice() else {
        return Err(invalid("unexpected offset row shape"));
    };
    let partition = partition
        .parse::<i32>()
        .map_err(|_| invalid("invalid offset partition"))?;
    if *group_id != target.group_id || *topic != target.topic || partition != target.partition {
        return Err(invalid("non-authoritative offset row"));
    }
    Ok(BrokerStateObservation::ShareGroupOffset(
        BrokerShareGroupOffset {
            observation,
            operation_id: target.operation_id.clone(),
            group_id: target.group_id.clone(),
            topic: target.topic.clone(),
            partition,
            start_offset: optional_nonnegative(start_offset, "start offset")?,
            lag: optional_nonnegative(lag, "lag")?,
        },
    ))
}

fn optional_nonnegative(value: &str, field: &str) -> Result<Option<i64>, ObserverError> {
    if value == "-" {
        return Ok(None);
    }
    let parsed = value
        .parse::<i64>()
        .map_err(|_| invalid(&format!("invalid {field}")))?;
    if parsed < 0 {
        return Err(invalid(&format!("negative {field}")));
    }
    Ok(Some(parsed))
}

fn invalid(message: &str) -> ObserverError {
    ObserverError::InvalidBrokerState(format!("Kafka CLI Share-group snapshot: {message}"))
}
