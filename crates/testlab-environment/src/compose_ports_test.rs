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
fn reassignment_replaces_every_advertised_compose_port() {
    let ports = HostPorts::fixed(39_092, 3)
        .unwrap_or_else(|error| panic!("create fixed host ports: {error}"));
    let mut environment = vec![
        ("KAFKA_HOST_PORT".to_owned(), "1".to_owned()),
        ("KAFKA_HOST_PORT_1".to_owned(), "1".to_owned()),
        ("KAFKA_HOST_PORT_2".to_owned(), "2".to_owned()),
        ("KAFKA_HOST_PORT_3".to_owned(), "3".to_owned()),
    ];

    ports
        .apply_to(&mut environment)
        .unwrap_or_else(|error| panic!("apply host ports: {error}"));

    assert_eq!(environment[0].1, "39092");
    assert_eq!(environment[1].1, "39092");
    assert_eq!(environment[2].1, "39093");
    assert_eq!(environment[3].1, "39094");
}

#[test]
fn reassignment_rejects_an_incomplete_compose_environment() {
    let ports = HostPorts::fixed(39_092, 2)
        .unwrap_or_else(|error| panic!("create fixed host ports: {error}"));
    let mut environment = vec![
        ("KAFKA_HOST_PORT".to_owned(), "1".to_owned()),
        ("KAFKA_HOST_PORT_1".to_owned(), "1".to_owned()),
    ];

    let error = match ports.apply_to(&mut environment) {
        Ok(()) => panic!("missing broker port must fail"),
        Err(error) => error,
    };

    assert_eq!(error.code(), "environment_host_port_invalid");
}

#[test]
fn named_ports_append_one_value_per_broker() {
    let ports = HostPorts::fixed(49_092, 2)
        .unwrap_or_else(|error| panic!("create fixed host ports: {error}"));
    let mut environment = Vec::new();

    ports.append_named(&mut environment, "KAFKA_BACKEND_HOST_PORT");

    assert_eq!(
        environment,
        [
            ("KAFKA_BACKEND_HOST_PORT".to_owned(), "49092".to_owned()),
            ("KAFKA_BACKEND_HOST_PORT_1".to_owned(), "49092".to_owned()),
            ("KAFKA_BACKEND_HOST_PORT_2".to_owned(), "49093".to_owned()),
        ]
    );
}

#[test]
fn empty_or_wrapping_ranges_are_rejected() {
    assert!(HostPorts::fixed(0, 1).is_err());
    assert!(HostPorts::fixed(u16::MAX, 2).is_err());
}
