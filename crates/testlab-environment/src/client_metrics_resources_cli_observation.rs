//! Pinned Kafka client-metrics CLI output becomes one canonical resource set.

use testlab_schema::{
    AdminConfigResource, BrokerConfigResourcesState, BrokerStateObservation, OperationId,
};

use crate::observer_error::ObserverError;

const CLIENT_METRICS_RESOURCE_TYPE: i8 = 16;

pub(super) fn normalize(
    observation: u64,
    operation_id: &OperationId,
    stdout: &[u8],
) -> Result<BrokerStateObservation, ObserverError> {
    let output = std::str::from_utf8(stdout).map_err(|_| invalid("output is not UTF-8"))?;
    let mut resources = if matches!(output, "" | "\n" | "\r\n") {
        Vec::new()
    } else {
        output
            .lines()
            .map(resource)
            .collect::<Result<Vec<_>, _>>()?
    };
    resources.sort_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));
    if resources
        .windows(2)
        .any(|pair| pair[0].name == pair[1].name)
    {
        return Err(invalid("reported a duplicate resource name"));
    }
    Ok(BrokerStateObservation::ConfigResources(
        BrokerConfigResourcesState {
            observation,
            operation_id: operation_id.clone(),
            resources,
        },
    ))
}

fn resource(name: &str) -> Result<AdminConfigResource, ObserverError> {
    if name.is_empty()
        || name.len() > 249
        || name
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err(invalid("reported an invalid resource name"));
    }
    Ok(AdminConfigResource {
        resource_type: CLIENT_METRICS_RESOURCE_TYPE,
        name: name.to_owned(),
    })
}

fn invalid(message: &str) -> ObserverError {
    ObserverError::InvalidBrokerState(format!(
        "Kafka CLI client-metrics resource snapshot: {message}"
    ))
}

#[cfg(test)]
#[path = "client_metrics_resources_cli_observation_test.rs"]
mod test;
