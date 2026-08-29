//! Proxy state applies typed controls and owns independently measured counters.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use testlab_schema::{NetworkDirection, NetworkFault, NetworkProxyRoute};

mod control;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RelayPolicy {
    Pass,
    Blackhole,
    Delay(Duration),
    Cut,
}

#[derive(Debug)]
pub(crate) struct SharedProxyState {
    inner: Mutex<ProxyState>,
    changed: Condvar,
}

#[derive(Debug)]
struct ProxyState {
    routes: BTreeMap<u16, RouteState>,
    next_connection: u64,
    next_observation: u64,
}

#[derive(Debug, Default)]
struct RouteState {
    active_connections: BTreeSet<u64>,
    cut_connections: BTreeSet<u64>,
    accepted_connections: u64,
    counters: Counters,
    fault: Option<ActiveFault>,
}

#[derive(Clone, Copy, Debug, Default)]
struct Counters {
    client_to_broker: u64,
    broker_to_client: u64,
    delayed_client_to_broker: u64,
    delayed_broker_to_client: u64,
    blocked_intervals: u64,
}

#[derive(Debug)]
struct ActiveFault {
    operation_id: testlab_schema::EnvironmentOperationId,
    fault: NetworkFault,
    started_unix_ms: u64,
    connections_at_start: u64,
    accepted_at_start: u64,
    counters_at_start: Counters,
}

impl SharedProxyState {
    pub(crate) fn new(routes: &[NetworkProxyRoute]) -> Result<Self, String> {
        let routes = routes
            .iter()
            .map(|route| (route.broker_ordinal, RouteState::default()))
            .collect::<BTreeMap<_, _>>();
        if routes.is_empty() {
            return Err("network proxy requires at least one route".to_owned());
        }
        Ok(Self {
            inner: Mutex::new(ProxyState {
                routes,
                next_connection: 0,
                next_observation: 0,
            }),
            changed: Condvar::new(),
        })
    }

    pub(crate) fn begin_connection(&self, broker: u16) -> Result<u64, String> {
        let mut state = self.lock()?;
        let connection = state.next_connection;
        state.next_connection = state
            .next_connection
            .checked_add(1)
            .ok_or_else(|| "network proxy connection identity overflowed".to_owned())?;
        let route = state.route_mut(broker)?;
        if route.active_connections.len() >= 256 {
            return Err(format!(
                "broker route {broker} exceeded 256 active connections"
            ));
        }
        route.active_connections.insert(connection);
        route.accepted_connections = route
            .accepted_connections
            .checked_add(1)
            .ok_or_else(|| "network proxy accepted connection count overflowed".to_owned())?;
        Ok(connection)
    }

    pub(crate) fn finish_connection(&self, broker: u16, connection: u64) {
        if let Ok(mut state) = self.inner.lock()
            && let Some(route) = state.routes.get_mut(&broker)
        {
            route.active_connections.remove(&connection);
            route.cut_connections.remove(&connection);
            self.changed.notify_all();
        }
    }

    pub(crate) fn policy(
        &self,
        broker: u16,
        connection: u64,
        direction: NetworkDirection,
    ) -> Result<RelayPolicy, String> {
        let mut state = self.lock()?;
        let route = state.route_mut(broker)?;
        if route.cut_connections.contains(&connection) {
            return Ok(RelayPolicy::Cut);
        }
        match route.fault.as_ref().map(|active| &active.fault) {
            Some(NetworkFault::Blackhole) => {
                route.counters.blocked_intervals = route
                    .counters
                    .blocked_intervals
                    .checked_add(1)
                    .ok_or_else(|| "network proxy blocked interval overflowed".to_owned())?;
                Ok(RelayPolicy::Blackhole)
            }
            Some(NetworkFault::Delay {
                direction: selected,
                delay_ms,
            }) if *selected == direction => {
                Ok(RelayPolicy::Delay(Duration::from_millis(*delay_ms)))
            }
            Some(NetworkFault::Delay { .. }) | None => Ok(RelayPolicy::Pass),
        }
    }

    pub(crate) fn record_forwarded(
        &self,
        broker: u16,
        direction: NetworkDirection,
        bytes: u64,
    ) -> Result<(), String> {
        let mut state = self.lock()?;
        let counters = &mut state.route_mut(broker)?.counters;
        let target = match direction {
            NetworkDirection::ClientToBroker => &mut counters.client_to_broker,
            NetworkDirection::BrokerToClient => &mut counters.broker_to_client,
        };
        *target = target
            .checked_add(bytes)
            .ok_or_else(|| "network proxy forwarded byte count overflowed".to_owned())?;
        Ok(())
    }

    pub(crate) fn record_delayed(
        &self,
        broker: u16,
        direction: NetworkDirection,
        bytes: u64,
    ) -> Result<(), String> {
        let mut state = self.lock()?;
        let counters = &mut state.route_mut(broker)?.counters;
        let target = match direction {
            NetworkDirection::ClientToBroker => &mut counters.delayed_client_to_broker,
            NetworkDirection::BrokerToClient => &mut counters.delayed_broker_to_client,
        };
        *target = target
            .checked_add(bytes)
            .ok_or_else(|| "network proxy delayed byte count overflowed".to_owned())?;
        Ok(())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, ProxyState>, String> {
        self.inner
            .lock()
            .map_err(|_| "network proxy state lock was poisoned".to_owned())
    }
}

impl ProxyState {
    fn route(&self, broker: u16) -> Result<&RouteState, String> {
        self.routes
            .get(&broker)
            .ok_or_else(|| format!("unknown broker route {broker}"))
    }

    fn route_mut(&mut self, broker: u16) -> Result<&mut RouteState, String> {
        self.routes
            .get_mut(&broker)
            .ok_or_else(|| format!("unknown broker route {broker}"))
    }

    fn next_observation(&mut self) -> Result<u64, String> {
        let value = self.next_observation;
        self.next_observation = value
            .checked_add(1)
            .ok_or_else(|| "network proxy observation identity overflowed".to_owned())?;
        Ok(value)
    }
}
