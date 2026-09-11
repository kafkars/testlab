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
mod test {
    use testlab_schema::{BrokerStateObservation, OperationId};

    #[test]
    fn normalizes_names_in_utf8_byte_order() {
        let state = super::normalize(7, &operation(), b"metrics-z\nmetrics-a\n")
            .unwrap_or_else(|error| panic!("normalize: {error}"));
        let BrokerStateObservation::ConfigResources(state) = state else {
            panic!("configuration-resource state");
        };
        assert_eq!(state.observation, 7);
        assert_eq!(state.operation_id, operation());
        assert_eq!(
            state
                .resources
                .iter()
                .map(|resource| (resource.resource_type, resource.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(16, "metrics-a"), (16, "metrics-z")]
        );
    }

    #[test]
    fn accepts_empty_output_and_rejects_ambiguous_names() {
        let empty = super::normalize(1, &operation(), b"\n")
            .unwrap_or_else(|error| panic!("empty listing: {error}"));
        let BrokerStateObservation::ConfigResources(empty) = empty else {
            panic!("configuration-resource state");
        };
        assert!(empty.resources.is_empty());
        assert!(super::normalize(1, &operation(), b"metrics-a\nmetrics-a\n").is_err());
        assert!(super::normalize(1, &operation(), b" metrics-a\n").is_err());
        assert!(super::normalize(1, &operation(), b"metrics\ta\n").is_err());
    }

    fn operation() -> OperationId {
        OperationId::new("admin-list-client-metrics-resources")
            .unwrap_or_else(|error| panic!("operation: {error}"))
    }
}
