//! TCP relay workers apply transport controls without parsing Kafka frames.

use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use testlab_schema::{NetworkDirection, NetworkProxyRoute};

use crate::network_proxy_state::{RelayPolicy, SharedProxyState};

const IO_POLL: Duration = Duration::from_millis(25);
const BLACKHOLE_POLL: Duration = Duration::from_millis(10);
const BUFFER_BYTES: usize = 16 * 1024;

pub(crate) fn serve(
    client: TcpStream,
    route: &NetworkProxyRoute,
    state: &Arc<SharedProxyState>,
    stopping: &Arc<AtomicBool>,
) -> Result<(), String> {
    let upstream = TcpStream::connect(&route.upstream_endpoint)
        .map_err(|error| format!("connect {}: {error}", route.upstream_endpoint))?;
    configure(&client)?;
    configure(&upstream)?;
    let client_read = clone(&client)?;
    let upstream_write = clone(&upstream)?;
    let connection = state.begin_connection(route.broker_ordinal)?;
    let forward_state = Arc::clone(state);
    let forward_stopping = Arc::clone(stopping);
    let broker = route.broker_ordinal;
    let forward = thread::Builder::new()
        .name(format!("testlab-proxy-{broker}-client"))
        .spawn(move || {
            relay(
                client_read,
                upstream_write,
                broker,
                connection,
                NetworkDirection::ClientToBroker,
                &forward_state,
                &forward_stopping,
            )
        });
    let forward = match forward {
        Ok(forward) => forward,
        Err(error) => {
            state.finish_connection(broker, connection);
            return Err(format!("spawn client relay: {error}"));
        }
    };
    let reverse = relay(
        upstream,
        client,
        broker,
        connection,
        NetworkDirection::BrokerToClient,
        state,
        stopping,
    );
    let forward = forward
        .join()
        .map_err(|_| "client relay panicked".to_owned())?;
    state.finish_connection(broker, connection);
    forward.and(reverse)
}

fn relay(
    mut reader: TcpStream,
    mut writer: TcpStream,
    broker: u16,
    connection: u64,
    direction: NetworkDirection,
    state: &SharedProxyState,
    stopping: &AtomicBool,
) -> Result<(), String> {
    let mut buffer = [0_u8; BUFFER_BYTES];
    while !stopping.load(Ordering::Acquire) {
        match await_read_policy(state, broker, connection, direction, stopping)? {
            RelayPolicy::Cut => break,
            RelayPolicy::Pass | RelayPolicy::Delay(_) => {}
            RelayPolicy::Blackhole => continue,
        }
        let read = match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) if transient(&error) => continue,
            Err(error) if disconnected(&error) => break,
            Err(error) => return Err(format!("read proxied connection: {error}")),
        };
        let policy = await_write_policy(state, broker, connection, direction, stopping)?;
        match policy {
            RelayPolicy::Cut => break,
            RelayPolicy::Delay(delay) => {
                state.record_delayed(broker, direction, read as u64)?;
                thread::sleep(delay);
            }
            RelayPolicy::Pass => {}
            RelayPolicy::Blackhole => unreachable!("write policy waits through blackholes"),
        }
        if let Err(error) = writer.write_all(&buffer[..read]) {
            if disconnected(&error) {
                break;
            }
            return Err(format!("write proxied connection: {error}"));
        }
        state.record_forwarded(broker, direction, read as u64)?;
    }
    let _ = reader.shutdown(Shutdown::Both);
    let _ = writer.shutdown(Shutdown::Both);
    Ok(())
}

fn await_read_policy(
    state: &SharedProxyState,
    broker: u16,
    connection: u64,
    direction: NetworkDirection,
    stopping: &AtomicBool,
) -> Result<RelayPolicy, String> {
    let policy = state.policy(broker, connection, direction)?;
    if policy == RelayPolicy::Blackhole && !stopping.load(Ordering::Acquire) {
        thread::sleep(BLACKHOLE_POLL);
    }
    Ok(policy)
}

fn await_write_policy(
    state: &SharedProxyState,
    broker: u16,
    connection: u64,
    direction: NetworkDirection,
    stopping: &AtomicBool,
) -> Result<RelayPolicy, String> {
    loop {
        let policy = state.policy(broker, connection, direction)?;
        if policy != RelayPolicy::Blackhole || stopping.load(Ordering::Acquire) {
            return Ok(policy);
        }
        thread::sleep(BLACKHOLE_POLL);
    }
}

fn configure(stream: &TcpStream) -> Result<(), String> {
    stream
        .set_nodelay(true)
        .and_then(|()| stream.set_read_timeout(Some(IO_POLL)))
        .and_then(|()| stream.set_write_timeout(Some(IO_POLL)))
        .map_err(|error| format!("configure proxied connection: {error}"))
}

fn clone(stream: &TcpStream) -> Result<TcpStream, String> {
    stream
        .try_clone()
        .map_err(|error| format!("clone proxied connection: {error}"))
}

fn transient(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut | io::ErrorKind::Interrupted
    )
}

fn disconnected(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::NotConnected
            | io::ErrorKind::UnexpectedEof
    )
}
