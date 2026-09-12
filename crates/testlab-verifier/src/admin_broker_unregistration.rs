//! Broker-unregistration verification binds public mutation to reversible cluster state.

use testlab_schema::{
    EnvironmentOperation, EnvironmentOperationKind, EnvironmentOperationStatus, OperationId,
    ScenarioAction, UnregisterBrokerAction, Violation,
};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::admin_lifecycle::Indexed;
use crate::index::{HistoryIndex, IndexedClusterDescription, IndexedClusterObservation};
use crate::support::violation;

pub(crate) fn verify(
    scenario_action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let window = index.admin_command_window(scenario_action);
    let ScenarioAction::UnregisterBroker(action) = scenario_action else {
        return false;
    };
    let public = one(index
        .admin_lifecycles
        .brokers_unregistered
        .get(&action.operation_id));
    let remaining = one(index.clusters_observed.get(&action.operation_id));
    let baseline = cluster_pair(index, &action.baseline_operation_id);
    let restored = cluster_pair(index, &action.restored_operation_id);
    let disruption = broker_disruption(index, action.broker_id);
    if exact_transition(
        action, window, public, remaining, baseline, restored, disruption,
    ) {
        return true;
    }
    violations.push(violation(
        "ADMIN-076",
        format!(
            "admin operation {} expected one public broker-{} unregistration, immediate exact remaining cluster state, and later restoration of the original cluster identity and broker set",
            action.operation_id, action.broker_id
        ),
        Some(action.operation_id.clone()),
        evidence(public, remaining, baseline, restored, disruption),
    ));
    true
}

fn exact_transition(
    action: &UnregisterBrokerAction,
    window: Option<AdminCommandWindow>,
    public: Option<&Indexed<testlab_schema::AdminBrokerUnregistration>>,
    remaining: Option<&IndexedClusterObservation>,
    baseline: Option<(&IndexedClusterDescription, &IndexedClusterObservation)>,
    restored: Option<(&IndexedClusterDescription, &IndexedClusterObservation)>,
    disruption: Option<Disruption<'_>>,
) -> bool {
    let (
        Some((command_sequence, _)),
        Some(public),
        Some(remaining),
        Some((baseline_public, baseline_observed)),
        Some((restored_public, restored_observed)),
        Some((stop, start)),
    ) = (window, public, remaining, baseline, restored, disruption)
    else {
        return false;
    };
    let mut complete_brokers = action.expected_remaining_broker_ids.clone();
    complete_brokers.push(action.broker_id);
    complete_brokers.sort_unstable();
    let cluster_id = baseline_public.cluster_id.as_deref();
    public.value.operation_id == action.operation_id
        && public.value.broker_id == action.broker_id
        && public.value.throttle_time_ms <= action.timeout_ms
        && public_after_command(window, public.history_sequence)
        && immediate_after_public(window, public.history_sequence, remaining.history_sequence)
        && remaining.broker_ids == action.expected_remaining_broker_ids
        && strictly_sorted(&remaining.broker_ids)
        && baseline_public.broker_ids == complete_brokers
        && baseline_observed.broker_ids == complete_brokers
        && restored_public.broker_ids == complete_brokers
        && restored_observed.broker_ids == complete_brokers
        && strictly_sorted(&complete_brokers)
        && cluster_id.is_some_and(|value| !value.is_empty())
        && baseline_observed.cluster_id.as_deref() == cluster_id
        && remaining.cluster_id.as_deref() == cluster_id
        && restored_public.cluster_id.as_deref() == cluster_id
        && restored_observed.cluster_id.as_deref() == cluster_id
        && baseline_public.history_sequence < baseline_observed.history_sequence
        && baseline_observed.history_sequence < stop.0
        && stop.0 < command_sequence
        && stop.1.status == EnvironmentOperationStatus::Succeeded
        && remaining.history_sequence < start.0
        && start.0 < restored_public.history_sequence
        && start.1.status == EnvironmentOperationStatus::Succeeded
        && restored_public.history_sequence < restored_observed.history_sequence
}

type Disruption<'a> = (
    &'a (u64, EnvironmentOperation),
    &'a (u64, EnvironmentOperation),
);

fn broker_disruption(index: &HistoryIndex, broker_id: i32) -> Option<Disruption<'_>> {
    let stop = one_operation(index, EnvironmentOperationKind::BrokerStop)?;
    let start = one_operation(index, EnvironmentOperationKind::BrokerStart)?;
    let stop_target = crate::group_recovery::disruption_target(&stop.1)?;
    let start_target = crate::group_recovery::disruption_target(&start.1)?;
    let expected_service = format!("broker-{broker_id}");
    (stop_target == start_target && stop_target.2 == expected_service.as_str())
        .then_some((stop, start))
}

fn one_operation(
    index: &HistoryIndex,
    kind: EnvironmentOperationKind,
) -> Option<&(u64, EnvironmentOperation)> {
    let mut operations = index
        .environment_operations
        .iter()
        .filter(|(_, operation)| operation.kind == kind);
    let operation = operations.next()?;
    operations.next().is_none().then_some(operation)
}

fn cluster_pair<'a>(
    index: &'a HistoryIndex,
    operation_id: &OperationId,
) -> Option<(&'a IndexedClusterDescription, &'a IndexedClusterObservation)> {
    Some((
        one(index.clusters_described.get(operation_id))?,
        one(index.clusters_observed.get(operation_id))?,
    ))
}

fn strictly_sorted(values: &[i32]) -> bool {
    !values.is_empty() && values.windows(2).all(|pair| pair[0] < pair[1])
}

fn one<T>(values: Option<&Vec<T>>) -> Option<&T> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}

fn evidence(
    public: Option<&Indexed<testlab_schema::AdminBrokerUnregistration>>,
    remaining: Option<&IndexedClusterObservation>,
    baseline: Option<(&IndexedClusterDescription, &IndexedClusterObservation)>,
    restored: Option<(&IndexedClusterDescription, &IndexedClusterObservation)>,
    disruption: Option<Disruption<'_>>,
) -> Vec<String> {
    let mut evidence = public
        .map(|value| format!("history:{}", value.history_sequence))
        .into_iter()
        .chain(
            [
                baseline.map(|(_, value)| value),
                remaining,
                restored.map(|(_, value)| value),
            ]
            .into_iter()
            .flatten()
            .map(|value| format!("broker-state-observation:{}", value.observation)),
        )
        .collect::<Vec<_>>();
    if let Some((stop, start)) = disruption {
        evidence.extend([
            format!("history:{}", stop.0),
            format!("history:{}", start.0),
        ]);
    }
    evidence
}
