//! Control transitions produce exact bounded network-fault observations.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use testlab_schema::{
    NetworkConnectionCutAction, NetworkConnectionsCutObservation, NetworkFaultAction,
    NetworkFaultState, NetworkFaultWindowObservation, NetworkProxyObservation,
};

use super::{ActiveFault, SharedProxyState};

impl SharedProxyState {
    pub(crate) fn alter(
        &self,
        action: &NetworkFaultAction,
    ) -> Result<Option<NetworkProxyObservation>, String> {
        match action.state {
            NetworkFaultState::Present => self.apply(action).map(|()| None),
            NetworkFaultState::Absent => self.remove(action).map(Some),
        }
    }

    fn apply(&self, action: &NetworkFaultAction) -> Result<(), String> {
        let mut state = self.lock()?;
        let route = state.route_mut(action.broker_ordinal)?;
        if route.fault.is_some() {
            return Err(format!(
                "broker route {} already has an active fault",
                action.broker_ordinal
            ));
        }
        route.fault = Some(ActiveFault {
            operation_id: action.operation_id.clone(),
            fault: action.fault.clone(),
            started_unix_ms: unix_ms()?,
            connections_at_start: route.active_connections.len() as u64,
            accepted_at_start: route.accepted_connections,
            counters_at_start: route.counters,
        });
        Ok(())
    }

    fn remove(&self, action: &NetworkFaultAction) -> Result<NetworkProxyObservation, String> {
        let mut state = self.lock()?;
        let observation = state.next_observation()?;
        let route = state.route_mut(action.broker_ordinal)?;
        let active = route
            .fault
            .take()
            .ok_or_else(|| format!("broker route {} has no active fault", action.broker_ordinal))?;
        if active.fault != action.fault {
            route.fault = Some(active);
            return Err(format!(
                "broker route {} fault removal did not match the active fault",
                action.broker_ordinal
            ));
        }
        fault_observation(
            observation,
            action,
            active,
            route.accepted_connections,
            route.counters,
        )
    }

    pub(crate) fn cut(
        &self,
        action: &NetworkConnectionCutAction,
    ) -> Result<NetworkProxyObservation, String> {
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(action.timeout_ms))
            .ok_or_else(|| "network cut deadline overflowed".to_owned())?;
        let mut state = self.lock()?;
        let targets = state
            .route_mut(action.broker_ordinal)?
            .active_connections
            .clone();
        state
            .route_mut(action.broker_ordinal)?
            .cut_connections
            .extend(&targets);
        while targets.iter().any(|id| {
            state
                .route(action.broker_ordinal)
                .is_ok_and(|route| route.active_connections.contains(id))
        }) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err("network cut did not close every selected connection".to_owned());
            }
            let waited = self
                .changed
                .wait_timeout(state, remaining)
                .map_err(|_| "network proxy state lock was poisoned".to_owned())?;
            state = waited.0;
        }
        let observation = state.next_observation()?;
        Ok(NetworkProxyObservation::ConnectionsCut(
            NetworkConnectionsCutObservation {
                observation,
                operation_id: action.operation_id.clone(),
                broker_ordinal: action.broker_ordinal,
                connections_cut: targets.len() as u64,
                completed_unix_ms: unix_ms()?,
            },
        ))
    }

    pub(crate) fn active_faults(&self) -> Result<Vec<u16>, String> {
        let state = self.lock()?;
        Ok(state
            .routes
            .iter()
            .filter_map(|(broker, route)| route.fault.as_ref().map(|_| *broker))
            .collect())
    }
}

fn fault_observation(
    observation: u64,
    action: &NetworkFaultAction,
    active: ActiveFault,
    accepted_connections: u64,
    counters: super::Counters,
) -> Result<NetworkProxyObservation, String> {
    Ok(NetworkProxyObservation::FaultWindow(
        NetworkFaultWindowObservation {
            observation,
            apply_operation_id: active.operation_id,
            remove_operation_id: action.operation_id.clone(),
            broker_ordinal: action.broker_ordinal,
            fault: active.fault,
            started_unix_ms: active.started_unix_ms,
            completed_unix_ms: unix_ms()?,
            connections_at_start: active.connections_at_start,
            connections_accepted: accepted_connections.saturating_sub(active.accepted_at_start),
            client_to_broker_bytes: counters
                .client_to_broker
                .saturating_sub(active.counters_at_start.client_to_broker),
            broker_to_client_bytes: counters
                .broker_to_client
                .saturating_sub(active.counters_at_start.broker_to_client),
            delayed_client_to_broker_bytes: counters
                .delayed_client_to_broker
                .saturating_sub(active.counters_at_start.delayed_client_to_broker),
            delayed_broker_to_client_bytes: counters
                .delayed_broker_to_client
                .saturating_sub(active.counters_at_start.delayed_broker_to_client),
            blocked_intervals: counters
                .blocked_intervals
                .saturating_sub(active.counters_at_start.blocked_intervals),
        },
    ))
}

fn unix_ms() -> Result<u64, String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("network proxy clock before Unix epoch: {error}"))?;
    u64::try_from(duration.as_millis()).map_err(|_| "network proxy Unix time overflowed".to_owned())
}
