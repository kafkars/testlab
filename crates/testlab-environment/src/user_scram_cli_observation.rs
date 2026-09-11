//! Pinned Kafka SCRAM CLI output becomes one exact named-user credential fact.

use testlab_schema::{
    BrokerStateObservation, BrokerUserScramCredentialState, OperationId, ScramCredentialMechanism,
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
    mechanism: ScramCredentialMechanism,
    stdout: &[u8],
) -> Result<BrokerStateObservation, ObserverError> {
    let output = std::str::from_utf8(stdout).map_err(|_| invalid("output is not UTF-8"))?;
    Ok(BrokerStateObservation::UserScramCredential(
        BrokerUserScramCredentialState {
            observation,
            operation_id: operation_id.clone(),
            user: user.to_owned(),
            mechanism,
            iterations: parse(user, mechanism, output)?,
        },
    ))
}

fn parse(
    user: &str,
    mechanism: ScramCredentialMechanism,
    output: &str,
) -> Result<Option<u32>, ObserverError> {
    let mut lines = output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty());
    let Some(mut line) = lines.next() else {
        return Err(invalid("output was empty"));
    };
    let quota_header = format!("Quota configs for user-principal '{user}' are");
    if let Some(values) = line.strip_prefix(&quota_header) {
        if !values.trim().is_empty() {
            return Err(invalid("reported unexpected quota values"));
        }
        line = lines
            .next()
            .ok_or_else(|| invalid("omitted the SCRAM result row"))?;
    }
    if lines.next().is_some() {
        return Err(invalid("reported multiple SCRAM result rows"));
    }
    let present = format!("SCRAM credential configs for user-principal '{user}' are ");
    if let Some(value) = line.strip_prefix(&present) {
        return parse_present(mechanism, value).map(Some);
    }
    let absent = format!(
        "Error retrieving SCRAM credential configs for user-principal '{user}': ExecutionException: org.apache.kafka.common.errors.ResourceNotFoundException:"
    );
    if line.starts_with(&absent) {
        return Ok(None);
    }
    Err(invalid("reported an unexpected SCRAM result row"))
}

fn parse_present(mechanism: ScramCredentialMechanism, value: &str) -> Result<u32, ObserverError> {
    if value.contains(',') {
        return Err(invalid("reported multiple SCRAM credentials"));
    }
    let expected = format!("{}=iterations=", mechanism.config_name());
    let iterations = value
        .strip_prefix(&expected)
        .ok_or_else(|| invalid("reported an unexpected SCRAM mechanism or shape"))?
        .parse::<u32>()
        .map_err(|_| invalid("reported non-numeric SCRAM iterations"))?;
    if !(4_096..=16_384).contains(&iterations) {
        return Err(invalid("reported out-of-range SCRAM iterations"));
    }
    Ok(iterations)
}

fn invalid(message: &str) -> ObserverError {
    ObserverError::InvalidBrokerState(format!("Kafka CLI user-SCRAM snapshot: {message}"))
}
