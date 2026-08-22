//! Host port tests prove stable multi-broker bootstrap discovery.

use crate::compose_ports::HostPorts;

#[test]
fn one_endpoint_is_emitted_per_broker() {
    let ports = HostPorts::fixed(29_092, 3)
        .unwrap_or_else(|error| panic!("create fixed host ports: {error}"));

    assert_eq!(ports.as_slice(), &[29_092, 29_093, 29_094]);
    assert_eq!(
        ports.endpoints(),
        ["127.0.0.1:29092", "127.0.0.1:29093", "127.0.0.1:29094"]
    );
    assert_eq!(
        ports.endpoint(),
        "127.0.0.1:29092,127.0.0.1:29093,127.0.0.1:29094"
    );
}

#[test]
fn empty_or_wrapping_ranges_are_rejected() {
    assert!(HostPorts::fixed(0, 1).is_err());
    assert!(HostPorts::fixed(u16::MAX, 2).is_err());
}
