//! Proxy-process tests exercise the versioned JSON Lines supervision boundary.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use testlab_schema::{
    EnvironmentOperationKind, EnvironmentOperationStatus, NetworkFault, NetworkFaultAction,
    NetworkFaultState, NetworkProxyControl, NetworkProxyObservation, NetworkProxyRoute,
};

use crate::{NetworkProxyProcessRequest, RunningNetworkProxy};

static FIXTURE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn child_controls_observations_and_terminal_artifacts_are_preserved() {
    let fixture = Fixture::new(script());
    let routes = [route()];
    let mut proxy = RunningNetworkProxy::start(
        &NetworkProxyProcessRequest {
            program: &fixture.program,
            repository_root: &fixture.root,
            routes: &routes,
            operation_id: operation("proxy-process"),
            started_unix_ms: 10,
        },
        Duration::from_secs(2),
    )
    .unwrap_or_else(|phase| panic!("start fake proxy: {:?}", phase.failure));

    proxy
        .control(
            &NetworkProxyControl::AlterFault(fault("blackhole-apply", NetworkFaultState::Present)),
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("apply blackhole: {error}"));
    proxy
        .control(
            &NetworkProxyControl::AlterFault(fault("blackhole-remove", NetworkFaultState::Absent)),
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("remove blackhole: {error}"));

    let observations = proxy.take_observations();
    assert!(matches!(
        observations.as_slice(),
        [NetworkProxyObservation::FaultWindow(window)]
            if window.apply_operation_id.as_str() == "blackhole-apply"
                && window.remove_operation_id.as_str() == "blackhole-remove"
                && window.blocked_intervals == 2
    ));
    let finish = proxy.finish(Duration::from_secs(2));
    assert!(finish.phase.succeeded(), "{:?}", finish.phase.failure);
    assert!(finish.observations.is_empty());
    let [terminal] = finish.phase.operations.as_slice() else {
        panic!("proxy must retain one process terminal");
    };
    assert_eq!(terminal.kind, EnvironmentOperationKind::NetworkProxy);
    assert_eq!(terminal.status, EnvironmentOperationStatus::Succeeded);
    assert_eq!(terminal.exit_code, Some(0));
    assert!(finish.phase.artifacts.iter().any(|artifact| {
        artifact.name == "network-proxy.jsonl"
            && String::from_utf8_lossy(&artifact.bytes).contains("fault_removed")
    }));
    assert!(finish.phase.artifacts.iter().any(|artifact| {
        artifact.name == "network-proxy.stderr.txt"
            && String::from_utf8_lossy(&artifact.bytes).contains("fixture diagnostic")
    }));
}

#[test]
fn mismatched_acknowledgement_poisons_the_process_terminal() {
    let fixture = Fixture::new(mismatched_script());
    let routes = [route()];
    let mut proxy = RunningNetworkProxy::start(
        &NetworkProxyProcessRequest {
            program: &fixture.program,
            repository_root: &fixture.root,
            routes: &routes,
            operation_id: operation("proxy-process"),
            started_unix_ms: 10,
        },
        Duration::from_secs(2),
    )
    .unwrap_or_else(|phase| panic!("start fake proxy: {:?}", phase.failure));

    let error = match proxy.control(
        &NetworkProxyControl::AlterFault(fault("blackhole-apply", NetworkFaultState::Present)),
        Duration::from_secs(1),
    ) {
        Ok(()) => panic!("a mismatched acknowledgement must fail"),
        Err(error) => error,
    };
    assert_eq!(error.code(), "network_proxy_protocol_failed");

    let finish = proxy.finish(Duration::from_secs(2));
    assert!(!finish.phase.succeeded());
    assert_eq!(
        finish.phase.operations[0].status,
        EnvironmentOperationStatus::Failed
    );
}

struct Fixture {
    root: PathBuf,
    program: PathBuf,
}

impl Fixture {
    fn new(source: &str) -> Self {
        let sequence = FIXTURE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "testlab-network-proxy-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&root)
            .unwrap_or_else(|error| panic!("create proxy fixture directory: {error}"));
        let program = root.join("fake-network-proxy");
        fs::write(&program, source)
            .unwrap_or_else(|error| panic!("write fake network proxy: {error}"));
        make_executable(&program);
        Self { root, program }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn script() -> &'static str {
    r#"#!/bin/sh
printf '%s\n' '{"event":"ready","protocol_version":1,"routes":[{"broker_ordinal":1,"listen_endpoint":"127.0.0.1:29092","upstream_endpoint":"127.0.0.1:39092"}]}'
IFS= read -r apply
printf '%s\n' '{"event":"fault_applied","protocol_version":1,"operation_id":"blackhole-apply"}'
IFS= read -r remove
printf '%s\n' '{"event":"fault_removed","protocol_version":1,"observation":{"observation_kind":"fault_window","observation":0,"apply_operation_id":"blackhole-apply","remove_operation_id":"blackhole-remove","broker_ordinal":1,"fault":{"fault_kind":"blackhole"},"started_unix_ms":11,"completed_unix_ms":12,"connections_at_start":1,"connections_accepted":0,"client_to_broker_bytes":0,"broker_to_client_bytes":0,"delayed_client_to_broker_bytes":0,"delayed_broker_to_client_bytes":0,"blocked_intervals":2}}'
printf '%s\n' 'fixture diagnostic' >&2
while IFS= read -r line; do :; done
"#
}

fn mismatched_script() -> &'static str {
    r#"#!/bin/sh
printf '%s\n' '{"event":"ready","protocol_version":1,"routes":[{"broker_ordinal":1,"listen_endpoint":"127.0.0.1:29092","upstream_endpoint":"127.0.0.1:39092"}]}'
IFS= read -r apply
printf '%s\n' '{"event":"fault_applied","protocol_version":1,"operation_id":"wrong-operation"}'
while IFS= read -r line; do :; done
"#
}

fn route() -> NetworkProxyRoute {
    NetworkProxyRoute {
        broker_ordinal: 1,
        listen_endpoint: "127.0.0.1:29092".to_owned(),
        upstream_endpoint: "127.0.0.1:39092".to_owned(),
    }
}

fn fault(id: &str, state: NetworkFaultState) -> NetworkFaultAction {
    NetworkFaultAction {
        operation_id: operation(id),
        broker_ordinal: 1,
        fault: NetworkFault::Blackhole,
        state,
        timeout_ms: 1_000,
    }
}

fn operation(value: &str) -> testlab_schema::EnvironmentOperationId {
    testlab_schema::EnvironmentOperationId::new(value)
        .unwrap_or_else(|error| panic!("environment operation id: {error}"))
}

#[cfg(unix)]
fn make_executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = fs::metadata(path)
        .unwrap_or_else(|error| panic!("inspect fake proxy: {error}"))
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)
        .unwrap_or_else(|error| panic!("make fake proxy executable: {error}"));
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) {}
