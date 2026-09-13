//! Configuration-resource listings join public identities to independent Kafka state.

use std::collections::BTreeSet;

use testlab_schema::{
    AdminConfigResource, ConfigResourceListingApi, ListConfigResourcesAction, ScenarioAction,
    Violation,
};

use crate::admin::{immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify_config_resources_action(
    scenario_action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let ScenarioAction::ListConfigResources(action) = scenario_action else {
        return false;
    };
    let window = index.admin_command_window(scenario_action);
    let public = one(index
        .admin_config_resources
        .listings
        .get(&action.operation_id));
    match action.api {
        ConfigResourceListingApi::Resource => {
            verify_topic_resources(action, index, public, window, violations);
        }
        ConfigResourceListingApi::ClientMetrics => {
            verify_client_metrics_resources(action, index, public, window, violations);
        }
    }
    true
}

fn verify_topic_resources(
    action: &ListConfigResourcesAction,
    index: &HistoryIndex,
    public: Option<&crate::index::admin_config_resources::IndexedConfigResourcesListing>,
    window: Option<crate::admin::AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let independent = index.topics_observed.get(&action.operation_id);
    let required = action
        .required_resources
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let independently_present = independent
        .into_iter()
        .flatten()
        .filter(|value| value.exists)
        .map(|value| value.topic.as_str())
        .collect::<BTreeSet<_>>();
    let public_matches = public.is_some_and(|value| {
        public_after_command(window, value.history_sequence)
            && value.throttle_time_ms <= action.timeout_ms
            && strictly_sorted(&value.resources)
            && value
                .resources
                .iter()
                .all(|resource| resource.resource_type == 2)
            && required.iter().all(|topic| {
                value
                    .resources
                    .binary_search_by(|resource| {
                        (resource.resource_type, resource.name.as_str()).cmp(&(2_i8, *topic))
                    })
                    .is_ok()
            })
    });
    let independent_matches = public.is_some_and(|public| {
        independent.is_some_and(|values| {
            values.len() == required.len()
                && independently_present == required
                && contiguous(values.iter().map(|value| value.observation))
                && contiguous(values.iter().map(|value| value.history_sequence))
                && values.iter().all(|value| {
                    value.exists
                        && required.contains(value.topic.as_str())
                        && immediate_after_public(
                            window,
                            public.history_sequence,
                            value.history_sequence,
                        )
                })
        })
    });
    if public_matches && independent_matches {
        return;
    }
    violations.push(violation(
        "ADMIN-063",
        format!(
            "admin operation {} expected one canonical topic-resource listing containing every independently present required topic",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        public
            .map(|value| format!("history:{}", value.history_sequence))
            .into_iter()
            .chain(independent.into_iter().flatten().map(|value| {
                format!("broker-state-observation:{}", value.observation)
            }))
            .collect(),
    ));
}

fn verify_client_metrics_resources(
    action: &ListConfigResourcesAction,
    index: &HistoryIndex,
    public: Option<&crate::index::admin_config_resources::IndexedConfigResourcesListing>,
    window: Option<crate::admin::AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let independent = one(index
        .admin_config_resources
        .observed
        .get(&action.operation_id));
    let mut expected = action
        .required_resources
        .iter()
        .cloned()
        .map(|name| AdminConfigResource {
            resource_type: 16,
            name,
        })
        .collect::<Vec<_>>();
    expected.sort();
    let matches = public.is_some_and(|public| {
        independent.is_some_and(|independent| {
            public.throttle_time_ms <= action.timeout_ms
                && strictly_sorted(&public.resources)
                && public.resources == expected
                && independent.value.resources == expected
                && public.resources == independent.value.resources
                && public_after_command(window, public.history_sequence)
                && immediate_after_public(
                    window,
                    public.history_sequence,
                    independent.history_sequence,
                )
        })
    });
    if matches {
        return;
    }
    violations.push(violation(
        "ADMIN-071",
        format!(
            "admin operation {} expected one canonical dedicated client-metrics resource listing exactly matching one immediate Kafka CLI snapshot",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        public
            .map(|value| format!("history:{}", value.history_sequence))
            .into_iter()
            .chain(independent.map(|value| {
                format!("broker-state-observation:{}", value.value.observation)
            }))
            .collect(),
    ));
}

fn strictly_sorted(resources: &[testlab_schema::AdminConfigResource]) -> bool {
    resources.windows(2).all(|pair| pair[0] < pair[1])
}

fn contiguous(values: impl Iterator<Item = u64>) -> bool {
    let mut previous: Option<u64> = None;
    for value in values {
        if previous.is_some_and(|previous| value != previous.saturating_add(1)) {
            return false;
        }
        previous = Some(value);
    }
    true
}

fn one<T>(values: Option<&Vec<T>>) -> Option<&T> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}
