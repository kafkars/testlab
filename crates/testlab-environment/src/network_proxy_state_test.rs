//! Proxy-state tests prove measured effects and exact connection cuts.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use testlab_schema::{
    NetworkConnectionCutAction, NetworkDirection, NetworkFault, NetworkFaultAction,
    NetworkFaultState, NetworkProxyObservation, NetworkProxyRoute,
};

use crate::network_proxy_state::{RelayPolicy, SharedProxyState};

#[test]
fn delay_window_reports_only_selected_direction_bytes() {
    let state = SharedProxyState::new(&[route()])
        .unwrap_or_else(|error| panic!("create proxy state: {error}"));
    let connection = state
        .begin_connection(1)
        .unwrap_or_else(|error| panic!("begin connection: {error}"));
    state
        .alter(&fault(
            "delay-apply",
            NetworkFaultState::Present,
            NetworkFault::Delay {
                direction: NetworkDirection::ClientToBroker,
                delay_ms: 25,
            },
        ))
        .unwrap_or_else(|error| panic!("apply delay: {error}"));

    assert_eq!(
        state.policy(1, connection, NetworkDirection::ClientToBroker),
        Ok(RelayPolicy::Delay(Duration::from_millis(25)))
    );
    assert_eq!(
        state.policy(1, connection, NetworkDirection::BrokerToClient),
        Ok(RelayPolicy::Pass)
    );
    state
        .record_delayed(1, NetworkDirection::ClientToBroker, 13)
        .unwrap_or_else(|error| panic!("record delayed bytes: {error}"));
    state
        .record_forwarded(1, NetworkDirection::ClientToBroker, 13)
        .unwrap_or_else(|error| panic!("record forwarded bytes: {error}"));

    let observation = state
        .alter(&fault(
            "delay-remove",
            NetworkFaultState::Absent,
            NetworkFault::Delay {
                direction: NetworkDirection::ClientToBroker,
                delay_ms: 25,
            },
        ))
        .unwrap_or_else(|error| panic!("remove delay: {error}"));
    let Some(NetworkProxyObservation::FaultWindow(window)) = observation else {
        panic!("delay removal must emit one fault window");
    };

    assert_eq!(window.observation, 0);
    assert_eq!(window.connections_at_start, 1);
    assert_eq!(window.delayed_client_to_broker_bytes, 13);
    assert_eq!(window.delayed_broker_to_client_bytes, 0);
    assert_eq!(window.client_to_broker_bytes, 13);
}

#[test]
fn blackhole_and_mismatched_removal_fail_closed() {
    let state = SharedProxyState::new(&[route()])
        .unwrap_or_else(|error| panic!("create proxy state: {error}"));
    let connection = state
        .begin_connection(1)
        .unwrap_or_else(|error| panic!("begin connection: {error}"));
    state
        .alter(&fault(
            "blackhole-apply",
            NetworkFaultState::Present,
            NetworkFault::Blackhole,
        ))
        .unwrap_or_else(|error| panic!("apply blackhole: {error}"));

    assert_eq!(
        state.policy(1, connection, NetworkDirection::ClientToBroker),
        Ok(RelayPolicy::Blackhole)
    );
    assert!(
        state
            .alter(&fault(
                "wrong-remove",
                NetworkFaultState::Absent,
                NetworkFault::Delay {
                    direction: NetworkDirection::ClientToBroker,
                    delay_ms: 25,
                },
            ))
            .is_err()
    );
    let observation = state
        .alter(&fault(
            "blackhole-remove",
            NetworkFaultState::Absent,
            NetworkFault::Blackhole,
        ))
        .unwrap_or_else(|error| panic!("remove blackhole: {error}"));
    let Some(NetworkProxyObservation::FaultWindow(window)) = observation else {
        panic!("blackhole removal must emit one fault window");
    };
    assert_eq!(window.blocked_intervals, 1);
}

#[test]
fn cut_waits_until_the_exact_active_connection_closes() {
    let state = Arc::new(
        SharedProxyState::new(&[route()])
            .unwrap_or_else(|error| panic!("create proxy state: {error}")),
    );
    let connection = state
        .begin_connection(1)
        .unwrap_or_else(|error| panic!("begin connection: {error}"));
    let cut_state = Arc::clone(&state);
    let cut = thread::spawn(move || {
        cut_state.cut(&NetworkConnectionCutAction {
            operation_id: operation("cut-live"),
            broker_ordinal: 1,
            timeout_ms: 1_000,
        })
    });

    let deadline = Instant::now() + Duration::from_millis(250);
    while state
        .policy(1, connection, NetworkDirection::ClientToBroker)
        .is_ok_and(|policy| policy != RelayPolicy::Cut)
        && Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        state.policy(1, connection, NetworkDirection::ClientToBroker),
        Ok(RelayPolicy::Cut)
    );
    state.finish_connection(1, connection);

    let observation = cut
        .join()
        .unwrap_or_else(|_| panic!("cut worker panicked"))
        .unwrap_or_else(|error| panic!("cut connections: {error}"));
    let NetworkProxyObservation::ConnectionsCut(cut) = observation else {
        panic!("cut must emit a cut observation");
    };
    assert_eq!(cut.connections_cut, 1);
    assert_eq!(cut.observation, 0);
}

fn route() -> NetworkProxyRoute {
    NetworkProxyRoute {
        broker_ordinal: 1,
        listen_endpoint: "127.0.0.1:29092".to_owned(),
        upstream_endpoint: "127.0.0.1:39092".to_owned(),
    }
}

fn fault(id: &str, state: NetworkFaultState, fault: NetworkFault) -> NetworkFaultAction {
    NetworkFaultAction {
        operation_id: operation(id),
        broker_ordinal: 1,
        fault,
        state,
        timeout_ms: 1_000,
    }
}

fn operation(value: &str) -> testlab_schema::EnvironmentOperationId {
    testlab_schema::EnvironmentOperationId::new(value)
        .unwrap_or_else(|error| panic!("environment operation id: {error}"))
}
