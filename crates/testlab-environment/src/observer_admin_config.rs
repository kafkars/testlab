//! Topic-configuration observation uses an independent librdkafka admin client.

use std::thread;
use std::time::{Duration, Instant};

use futures_executor::block_on;
use rdkafka::admin::{
    AdminOptions, ConfigResourceResult, OwnedResourceSpecifier, ResourceSpecifier,
};
use testlab_schema::{BrokerStateObservation, BrokerTopicConfigState};

use crate::observer::remaining;
use crate::observer_admin::{AdminObserverRequest, client};
use crate::observer_admin_target::{ConfigBatchTarget, ConfigTarget, ordinal};
use crate::observer_error::ObserverError;

const POLL_SLICE: Duration = Duration::from_millis(50);

pub(super) fn capture_batch(
    request: AdminObserverRequest<'_>,
    target: &ConfigBatchTarget,
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    target
        .configs
        .iter()
        .enumerate()
        .map(|(index, selected)| {
            capture(
                AdminObserverRequest {
                    first_observation: ordinal(request.first_observation, index)?,
                    ..request
                },
                selected,
            )
        })
        .collect()
}

pub(super) fn capture(
    request: AdminObserverRequest<'_>,
    target: &ConfigTarget,
) -> Result<BrokerStateObservation, ObserverError> {
    let admin = client(request, "topic-config")?;
    let brokers = if target.poll_expected {
        let metadata = admin
            .inner()
            .fetch_metadata(None, remaining(request.deadline)?)?;
        config_brokers(
            metadata
                .brokers()
                .iter()
                .map(rdkafka::metadata::MetadataBroker::id)
                .collect(),
            request.cluster_size,
        )?
    } else {
        vec![None]
    };
    capture_from_brokers(target, request.deadline, &brokers, |broker_id| {
        let resource = ResourceSpecifier::Topic(&target.topic);
        let options = AdminOptions::new()
            .broker_id(broker_id)
            .request_timeout(Some(remaining(request.deadline)?));
        let results = block_on(admin.describe_configs([&resource], &options))?;
        normalize(request.first_observation, target, results)
    })
}

pub(super) fn config_brokers(
    mut broker_ids: Vec<i32>,
    cluster_size: u16,
) -> Result<Vec<Option<i32>>, ObserverError> {
    broker_ids.sort_unstable();
    if broker_ids.is_empty()
        || broker_ids.len() != usize::from(cluster_size)
        || broker_ids.iter().any(|id| *id < 0)
        || broker_ids.windows(2).any(|pair| pair[0] == pair[1])
    {
        return Err(ObserverError::InvalidBrokerState(format!(
            "topic configuration metadata must expose {cluster_size} distinct valid brokers"
        )));
    }
    Ok(broker_ids.into_iter().map(Some).collect())
}

/// Mutation barriers cover every broker that a later public read can select.
/// Query observations still retain the first independent result without polling.
pub(super) fn capture_from_brokers(
    target: &ConfigTarget,
    deadline: Instant,
    brokers: &[Option<i32>],
    mut describe: impl FnMut(Option<i32>) -> Result<BrokerStateObservation, ObserverError>,
) -> Result<BrokerStateObservation, ObserverError> {
    loop {
        let mut pending = false;
        let mut last = None;
        for broker in brokers {
            remaining(deadline)?;
            let observed = describe(*broker)?;
            if !target.poll_expected {
                return Ok(observed);
            }
            pending |= observed_value(&observed) != Some(target.expected_value.as_str());
            last = Some(observed);
        }
        if !pending {
            return last.ok_or_else(|| invalid(target, "had no brokers to observe"));
        }
        let wait = remaining(deadline)?;
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
