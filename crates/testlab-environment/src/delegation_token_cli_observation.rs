//! Secret-free Kafka CLI projections retain only owner-filtered token counts.

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
mod tests {
    use super::*;

    #[test]
    fn accepts_one_secret_free_count_and_rejects_all_other_shapes() {
        let operation = OperationId::new("delegation-token-lifecycle")
            .unwrap_or_else(|error| panic!("operation id: {error}"));
        let owner = DelegationTokenPrincipalSpec {
            principal_type: "User".to_owned(),
            principal_name: "kafkars".to_owned(),
        };
        let state = normalize(4, &operation, &owner, b"token-count:0\n")
            .unwrap_or_else(|error| panic!("token state: {error}"));
        assert_eq!(state.token_count, 0);
        assert_eq!(state.owner, owner);
        assert!(normalize(4, &operation, &owner, b"TOKENID HMAC secret\n").is_err());
        assert!(normalize(4, &operation, &owner, b"token-count:1\ntoken-count:0\n").is_err());
    }
}
