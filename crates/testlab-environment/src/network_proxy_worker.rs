//! External proxy worker owns loopback listeners and its JSON Lines control loop.

use std::collections::BTreeSet;
use std::io::{self, BufRead};
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use testlab_schema::{
    NETWORK_PROXY_PROTOCOL_VERSION, NetworkFaultState, NetworkProxyControl,
    NetworkProxyControlEnvelope, NetworkProxyEvent, NetworkProxyRoute,
};

use crate::network_proxy_output::NetworkEventWriter;
use crate::network_proxy_relay;
use crate::network_proxy_state::SharedProxyState;

const MAX_CONTROL_LINE_BYTES: usize = 64 * 1024;

/// Runs the protocol-only network proxy child until control stdin reaches EOF.
pub fn run_network_proxy_worker(route_args: &[String]) -> Result<(), String> {
    let routes = parse_routes(route_args)?;
    let listeners = bind_routes(&routes)?;
    let state = Arc::new(SharedProxyState::new(&routes)?);
    let output = NetworkEventWriter::default();
    let stopping = Arc::new(AtomicBool::new(false));
    let acceptors = listeners
        .into_iter()
        .map(|(route, listener)| {
            spawn_acceptor(
                route,
                listener,
                Arc::clone(&state),
                output.clone(),
                Arc::clone(&stopping),
            )
        })
        .collect::<Vec<_>>();
    output.emit(&NetworkProxyEvent::Ready {
        protocol_version: NETWORK_PROXY_PROTOCOL_VERSION,
        routes: routes.clone(),
    })?;
    let controls = read_controls(&state, &output);
    stopping.store(true, Ordering::Release);
    let acceptors = join_acceptors(acceptors);
    if let Err(diagnostic) = controls.as_ref().or(acceptors.as_ref()) {
        let _ = output.emit(&NetworkProxyEvent::Fatal {
            protocol_version: NETWORK_PROXY_PROTOCOL_VERSION,
            code: "network_proxy_failed".to_owned(),
            diagnostic: diagnostic.clone(),
        });
    }
    controls?;
    acceptors?;
    let active = state.active_faults()?;
    if !active.is_empty() {
        return Err(format!(
            "network proxy faults remained active on broker routes {active:?}"
        ));
    }
    Ok(())
}

fn parse_routes(values: &[String]) -> Result<Vec<NetworkProxyRoute>, String> {
    if values.is_empty() {
        return Err("network proxy received no routes".to_owned());
    }
    let mut ordinals = BTreeSet::new();
    let mut listeners = BTreeSet::new();
    let mut routes = values
        .iter()
        .map(|value| parse_route(value))
        .collect::<Result<Vec<_>, _>>()?;
    routes.sort_by_key(|route| route.broker_ordinal);
    for route in &routes {
        if !ordinals.insert(route.broker_ordinal) {
            return Err(format!(
                "duplicate network proxy broker route {}",
                route.broker_ordinal
            ));
        }
        if !listeners.insert(route.listen_endpoint.clone()) {
            return Err(format!(
                "duplicate network proxy listener {}",
                route.listen_endpoint
            ));
        }
    }
    Ok(routes)
}

fn parse_route(value: &str) -> Result<NetworkProxyRoute, String> {
    let parts = value.split('|').collect::<Vec<_>>();
    let [ordinal, listen, upstream] = parts.as_slice() else {
        return Err(format!(
            "network proxy route must be ORDINAL|LISTEN|UPSTREAM: {value}"
        ));
    };
    let broker_ordinal = ordinal
        .parse::<u16>()
        .map_err(|error| format!("invalid network proxy broker ordinal: {error}"))?;
    if broker_ordinal == 0 {
        return Err("network proxy broker ordinal must be one-based".to_owned());
    }
    let listen_endpoint = loopback_endpoint(listen)?;
    let upstream_endpoint = loopback_endpoint(upstream)?;
    if listen_endpoint == upstream_endpoint {
        return Err("network proxy listener and upstream must differ".to_owned());
    }
    Ok(NetworkProxyRoute {
        broker_ordinal,
        listen_endpoint,
        upstream_endpoint,
    })
}

fn loopback_endpoint(value: &str) -> Result<String, String> {
    let address = value
        .parse::<SocketAddr>()
        .map_err(|error| format!("invalid network proxy endpoint {value}: {error}"))?;
    if !address.ip().is_loopback() || address.port() == 0 {
        return Err(format!(
            "network proxy endpoint must use loopback and a nonzero port: {value}"
        ));
    }
    Ok(address.to_string())
}

fn bind_routes(
    routes: &[NetworkProxyRoute],
) -> Result<Vec<(NetworkProxyRoute, TcpListener)>, String> {
    routes
        .iter()
        .map(|route| {
            let listener = TcpListener::bind(&route.listen_endpoint)
                .map_err(|error| format!("bind {}: {error}", route.listen_endpoint))?;
            listener
                .set_nonblocking(true)
                .map_err(|error| format!("configure {}: {error}", route.listen_endpoint))?;
            Ok((route.clone(), listener))
        })
        .collect()
}

fn spawn_acceptor(
    route: NetworkProxyRoute,
    listener: TcpListener,
    state: Arc<SharedProxyState>,
    output: NetworkEventWriter,
    stopping: Arc<AtomicBool>,
) -> thread::JoinHandle<Result<(), String>> {
    thread::spawn(move || {
        let mut connections = Vec::new();
        while !stopping.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((client, _)) => {
                    let route = route.clone();
                    let state = Arc::clone(&state);
                    let stopping = Arc::clone(&stopping);
                    connections.push(thread::spawn(move || {
                        if let Err(error) =
                            network_proxy_relay::serve(client, &route, &state, &stopping)
                        {
                            eprintln!("network proxy connection ended: {error}");
                        }
                    }));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => {
                    let diagnostic = format!("accept {}: {error}", route.listen_endpoint);
                    let _ = output.emit(&NetworkProxyEvent::Fatal {
                        protocol_version: NETWORK_PROXY_PROTOCOL_VERSION,
                        code: "network_proxy_accept_failed".to_owned(),
                        diagnostic: diagnostic.clone(),
                    });
                    stopping.store(true, Ordering::Release);
                    return Err(diagnostic);
                }
            }
        }
        for connection in connections {
            connection
                .join()
                .map_err(|_| "network proxy connection worker panicked".to_owned())?;
        }
        Ok(())
    })
}

fn join_acceptors(acceptors: Vec<thread::JoinHandle<Result<(), String>>>) -> Result<(), String> {
    for acceptor in acceptors {
        acceptor
            .join()
            .map_err(|_| "network proxy acceptor panicked".to_owned())??;
    }
    Ok(())
}

fn read_controls(state: &SharedProxyState, output: &NetworkEventWriter) -> Result<(), String> {
    let stdin = io::stdin();
    let mut reader = stdin.lock();
    while let Some(line) = read_bounded_line(&mut reader)? {
        let envelope: NetworkProxyControlEnvelope = serde_json::from_str(&line)
            .map_err(|error| format!("decode network proxy control: {error}"))?;
        if envelope.protocol_version != NETWORK_PROXY_PROTOCOL_VERSION {
            return Err(format!(
                "unsupported network proxy protocol version {}",
                envelope.protocol_version
            ));
        }
        apply_control(state, output, envelope.control)?;
    }
    Ok(())
}

fn apply_control(
    state: &SharedProxyState,
    output: &NetworkEventWriter,
    control: NetworkProxyControl,
) -> Result<(), String> {
    match control {
        NetworkProxyControl::AlterFault(action) => match state.alter(&action)? {
            None if action.state == NetworkFaultState::Present => {
                output.emit(&NetworkProxyEvent::FaultApplied {
                    protocol_version: NETWORK_PROXY_PROTOCOL_VERSION,
                    operation_id: action.operation_id,
                })
            }
            Some(observation) if action.state == NetworkFaultState::Absent => {
                output.emit(&NetworkProxyEvent::FaultRemoved {
                    protocol_version: NETWORK_PROXY_PROTOCOL_VERSION,
                    observation,
                })
            }
            _ => Err("network proxy fault transition returned an invalid shape".to_owned()),
        },
        NetworkProxyControl::CutConnections(action) => {
            let observation = state.cut(&action)?;
            output.emit(&NetworkProxyEvent::ConnectionsCut {
                protocol_version: NETWORK_PROXY_PROTOCOL_VERSION,
                observation,
            })
        }
    }
}

fn read_bounded_line<R: BufRead>(reader: &mut R) -> Result<Option<String>, String> {
    let mut bytes = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .map_err(|error| format!("read network proxy control: {error}"))?;
        if available.is_empty() {
            return if bytes.is_empty() {
                Ok(None)
            } else {
                Err("network proxy control line was incomplete".to_owned())
            };
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        bytes.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);
        if bytes.len() > MAX_CONTROL_LINE_BYTES {
            return Err("network proxy control line exceeded its bound".to_owned());
        }
        if newline.is_some() {
            let _ = bytes.pop();
            return String::from_utf8(bytes)
                .map(Some)
                .map_err(|error| format!("network proxy control was not UTF-8: {error}"));
        }
    }
}
