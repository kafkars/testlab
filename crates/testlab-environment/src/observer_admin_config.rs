//! Topic-configuration observation uses an independent librdkafka admin client.

use std::thread;
use std::time::Duration;

use futures_executor::block_on;
use rdkafka::admin::{
    AdminOptions, ConfigResourceResult, OwnedResourceSpecifier, ResourceSpecifier,
};
use testlab_schema::{BrokerStateObservation, BrokerTopicConfigState};

use crate::observer::remaining;
use crate::observer_admin::{AdminObserverRequest, client};
use crate::observer_admin_target::ConfigTarget;
use crate::observer_error::ObserverError;

const POLL_SLICE: Duration = Duration::from_millis(50);

pub(super) fn capture(
    request: AdminObserverRequest<'_>,
    target: &ConfigTarget,
) -> Result<BrokerStateObservation, ObserverError> {
    let admin = client(request, "topic-config")?;
    loop {
        let resource = ResourceSpecifier::Topic(&target.topic);
        let options = AdminOptions::new().request_timeout(Some(remaining(request.deadline)?));
        let results = block_on(admin.describe_configs([&resource], &options))?;
        let observed = normalize(request.first_observation, target, results)?;
        if !target.poll_expected
            || observed_value(&observed) == Some(target.expected_value.as_str())
        {
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

fn normalize(
    observation: u64,
    target: &ConfigTarget,
    results: Vec<ConfigResourceResult>,
) -> Result<BrokerStateObservation, ObserverError> {
    let mut results = results.into_iter();
    let resource = results
        .next()
        .ok_or_else(|| invalid(target, "returned no topic resource"))?
        .map_err(|error| invalid(target, format!("returned Kafka error {error}")))?;
    if results.next().is_some()
        || resource.specifier != OwnedResourceSpecifier::Topic(target.topic.clone())
    {
        return Err(invalid(target, "returned an unexpected topic resource"));
    }
    let mut entries = resource
        .entries
        .into_iter()
        .filter(|entry| entry.name == target.config_name);
    let entry = entries
        .next()
        .ok_or_else(|| invalid(target, "omitted the selected configuration"))?;
    if entries.next().is_some() {
        return Err(invalid(target, "repeated the selected configuration"));
    }
    if entry.is_sensitive {
        return Err(invalid(
            target,
            "marked the selected configuration sensitive",
        ));
    }
    let value = entry
        .value
        .ok_or_else(|| invalid(target, "returned no observable configuration value"))?;
    Ok(BrokerStateObservation::TopicConfig(
        BrokerTopicConfigState {
            observation,
            operation_id: target.operation_id.clone(),
            topic: target.topic.clone(),
            config_name: target.config_name.clone(),
            value,
        },
    ))
}

fn observed_value(observation: &BrokerStateObservation) -> Option<&str> {
    match observation {
        BrokerStateObservation::TopicConfig(value) => Some(&value.value),
        _ => None,
    }
}

fn invalid(target: &ConfigTarget, detail: impl std::fmt::Display) -> ObserverError {
    ObserverError::InvalidBrokerState(format!(
        "topic {} configuration {} {detail}",
        target.topic, target.config_name
    ))
}

#[cfg(test)]
pub(super) fn normalize_fixture(
    observation: u64,
    target: &ConfigTarget,
    topic: &str,
    entries: Vec<(&str, Option<&str>, bool)>,
) -> Result<BrokerStateObservation, ObserverError> {
    use rdkafka::admin::{ConfigEntry, ConfigResource, ConfigSource};

    normalize(
        observation,
        target,
        vec![Ok(ConfigResource {
            specifier: OwnedResourceSpecifier::Topic(topic.to_owned()),
            entries: entries
                .into_iter()
                .map(|(name, value, is_sensitive)| ConfigEntry {
                    name: name.to_owned(),
                    value: value.map(str::to_owned),
                    source: ConfigSource::DynamicTopic,
                    is_read_only: false,
                    is_default: false,
                    is_sensitive,
                })
                .collect(),
        })],
    )
}
