//! Cluster description verification compares exact public and independent identities.

use testlab_schema::{ScenarioAction, Violation};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::{HistoryIndex, IndexedClusterDescription, IndexedClusterObservation};
use crate::support::violation;

pub(crate) fn verify_cluster_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let command_window = index.admin_command_window(action);
    let ScenarioAction::DescribeCluster(action) = action else {
        return false;
    };
    let public = index.clusters_described.get(&action.operation_id);
    let independent = index.clusters_observed.get(&action.operation_id);
    if exact_match(public, independent, command_window) {
        return true;
    }
    violations.push(violation(
        "ADMIN-008",
        format!("admin operation {} expected one public cluster identity exactly matching one independent metadata snapshot", action.operation_id),
        Some(action.operation_id.clone()),
        evidence(public, independent),
    ));
    true
}

fn exact_match(
    public: Option<&Vec<IndexedClusterDescription>>,
    independent: Option<&Vec<IndexedClusterObservation>>,
    command_window: Option<AdminCommandWindow>,
) -> bool {
    let (Some(public), Some(independent)) = (public, independent) else {
        return false;
    };
    if public.len() != 1 || independent.len() != 1 {
        return false;
    }
    let (Some(public), Some(independent)) = (public.first(), independent.first()) else {
        return false;
    };
    public.cluster_id.is_some()
        && public.cluster_id == independent.cluster_id
        && strictly_sorted(&public.broker_ids)
        && public.broker_ids == independent.broker_ids
        && public_after_command(command_window, public.history_sequence)
        && immediate_after_public(
            command_window,
            public.history_sequence,
            independent.history_sequence,
        )
}

fn strictly_sorted(values: &[i32]) -> bool {
    !values.is_empty() && values.windows(2).all(|pair| pair[0] < pair[1])
}

fn evidence(
    public: Option<&Vec<IndexedClusterDescription>>,
    independent: Option<&Vec<IndexedClusterObservation>>,
) -> Vec<String> {
    public
        .into_iter()
        .flatten()
        .map(|value| format!("history:{}", value.history_sequence))
        .chain(
            independent
                .into_iter()
                .flatten()
                .map(|value| format!("broker-state-observation:{}", value.observation)),
        )
        .collect()
}
