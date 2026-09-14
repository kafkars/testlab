//! Pinned Kafka quota CLI output becomes one exact named-user byte-rate state fact.

use testlab_schema::{
    BrokerClientQuotaState, BrokerQuotaDirection, BrokerStateObservation, OperationId,
};

use crate::observer_error::ObserverError;

pub(super) fn selection(user: &str) -> Vec<String> {
    vec![
        "--entity-type".to_owned(),
        "users".to_owned(),
        "--entity-name".to_owned(),
        user.to_owned(),
    ]
}

pub(super) fn normalize(
    observation: u64,
    operation_id: &OperationId,
    user: &str,
    direction: BrokerQuotaDirection,
    stdout: &[u8],
) -> Result<BrokerStateObservation, ObserverError> {
    let output = std::str::from_utf8(stdout).map_err(|_| invalid("output is not UTF-8"))?;
    let rate = parse(user, direction, output)?;
    Ok(BrokerStateObservation::ClientQuota(
        BrokerClientQuotaState {
            observation,
            operation_id: operation_id.clone(),
            user: user.to_owned(),
            direction,
            bytes_per_second: rate,
        },
    ))
}

fn parse(
    user: &str,
    direction: BrokerQuotaDirection,
    output: &str,
) -> Result<Option<u64>, ObserverError> {
    if output.trim().is_empty() {
        return Ok(None);
    }
    let mut lines = output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty());
    let Some(line) = lines.next() else {
        return Ok(None);
    };
    if lines.next().is_some() {
        return Err(invalid("reported multiple output rows"));
    }
    let prefix = format!("Quota configs for user-principal '{user}' are");
    let Some(values) = line.strip_prefix(&prefix) else {
        return Err(invalid("omitted the exact user principal"));
    };
    let values = values.trim();
    if values.is_empty() {
        return Ok(None);
    }
    let mut values = values.split(',').map(str::trim);
    let Some(value) = values.next() else {
        return Err(invalid("omitted quota values"));
    };
    if values.next().is_some() {
        return Err(invalid("reported multiple quota values"));
    }
    let (key, value) = value
        .split_once('=')
        .ok_or_else(|| invalid("reported a malformed quota value"))?;
    if key != direction.config_name() {
        return Err(invalid("reported an unexpected quota key"));
    }
    Ok(Some(parse_rate(value)?))
}

fn parse_rate(value: &str) -> Result<u64, ObserverError> {
    let (whole, fraction) = match value.split_once('.') {
        Some((whole, fraction)) if !fraction.is_empty() => (whole, Some(fraction)),
        Some(_) => return Err(invalid("reported an empty decimal fraction")),
        None => (value, None),
    };
    if value.matches('.').count() > 1
        || fraction.is_some_and(|digits| digits.bytes().any(|digit| digit != b'0'))
    {
        return Err(invalid("reported a non-integral quota value"));
    }
    let rate = whole
        .parse::<u64>()
        .map_err(|_| invalid("reported a non-numeric quota value"))?;
    if !(1..=u64::from(u32::MAX)).contains(&rate) {
        return Err(invalid("reported an out-of-range quota value"));
    }
    Ok(rate)
}

fn invalid(message: &str) -> ObserverError {
    ObserverError::InvalidBrokerState(format!("Kafka CLI client-quota snapshot: {message}"))
}
