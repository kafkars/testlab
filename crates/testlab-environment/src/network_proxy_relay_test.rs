//! Relay tests prove transparent byte forwarding without Kafka parsing.

use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread;

use testlab_schema::NetworkProxyRoute;

use crate::network_proxy_state::SharedProxyState;

#[test]
#[ignore = "requires loopback socket binding"]
fn arbitrary_bytes_round_trip_through_the_external_route() {
    let upstream = TcpListener::bind(("127.0.0.1", 0))
        .unwrap_or_else(|error| panic!("bind upstream: {error}"));
    let upstream_endpoint = upstream
        .local_addr()
        .unwrap_or_else(|error| panic!("upstream address: {error}"))
        .to_string();
    let front =
        TcpListener::bind(("127.0.0.1", 0)).unwrap_or_else(|error| panic!("bind front: {error}"));
    let front_endpoint = front
        .local_addr()
        .unwrap_or_else(|error| panic!("front address: {error}"))
        .to_string();
    let route = NetworkProxyRoute {
        broker_ordinal: 1,
        listen_endpoint: front_endpoint.clone(),
        upstream_endpoint,
    };
    let state = Arc::new(
        SharedProxyState::new(std::slice::from_ref(&route))
            .unwrap_or_else(|error| panic!("create proxy state: {error}")),
    );
    let echo = thread::spawn(move || {
        let (mut stream, _) = upstream
            .accept()
            .unwrap_or_else(|error| panic!("accept upstream: {error}"));
        let mut bytes = [0_u8; 5];
        stream
            .read_exact(&mut bytes)
            .unwrap_or_else(|error| panic!("read upstream: {error}"));
        stream
            .write_all(&bytes)
            .unwrap_or_else(|error| panic!("write upstream: {error}"));
    });
    let relay_state = Arc::clone(&state);
    let relay = thread::spawn(move || {
        let (client, _) = front
            .accept()
            .unwrap_or_else(|error| panic!("accept front: {error}"));
        crate::network_proxy_relay::serve(
            client,
            &route,
            &relay_state,
            &Arc::new(AtomicBool::new(false)),
        )
    });

    let mut client =
        TcpStream::connect(front_endpoint).unwrap_or_else(|error| panic!("connect front: {error}"));
    client
        .write_all(b"\0\x01abc")
        .unwrap_or_else(|error| panic!("write client: {error}"));
    let mut echoed = [0_u8; 5];
    client
        .read_exact(&mut echoed)
        .unwrap_or_else(|error| panic!("read client: {error}"));
    assert_eq!(&echoed, b"\0\x01abc");
    let _ = client.shutdown(Shutdown::Both);

    echo.join()
        .unwrap_or_else(|_| panic!("echo worker panicked"));
    relay
        .join()
        .unwrap_or_else(|_| panic!("relay worker panicked"))
        .unwrap_or_else(|error| panic!("relay failed: {error}"));
    assert!(state.active_faults().is_ok_and(|faults| faults.is_empty()));
}
