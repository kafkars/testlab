//! Secret-free Kafka CLI projections retain only owner-filtered live-token counts.

use std::str;

use testlab_schema::{BrokerDelegationTokensState, DelegationTokenPrincipalSpec, OperationId};

use crate::observer_error::ObserverError;

const PREFIX: &str = "token-count:";

pub(crate) fn normalize(
    observation: u64,
    operation_id: &OperationId,
    owner: &DelegationTokenPrincipalSpec,
    stdout: &[u8],
) -> Result<BrokerDelegationTokensState, ObserverError> {
    let text =
        str::from_utf8(stdout).map_err(|_| invalid("sanitized token count was not valid UTF-8"))?;
    let mut lines = text.lines();
    let line = lines
        .next()
        .ok_or_else(|| invalid("sanitized token count was empty"))?;
    if lines.any(|candidate| !candidate.trim().is_empty()) {
        return Err(invalid("sanitized token count contained multiple rows"));
    }
    let token_count = line
        .trim()
        .strip_prefix(PREFIX)
        .and_then(|value| value.parse::<u32>().ok())
        .ok_or_else(|| invalid("sanitized token count was malformed"))?;
    Ok(BrokerDelegationTokensState {
        observation,
        operation_id: operation_id.clone(),
        owner: owner.clone(),
        token_count,
    })
}

fn invalid(detail: &str) -> ObserverError {
    ObserverError::InvalidBrokerState(format!("Kafka CLI delegation-token snapshot: {detail}"))
}

#[cfg(test)]
#[path = "delegation_token_cli_observation_test.rs"]
mod tests;
