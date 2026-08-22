//! Provisioning tests pin topology-sized topic replication evidence.

use std::collections::BTreeMap;

use crate::compose_provision::operation_args;

#[test]
fn operation_records_cluster_replication_factor() {
    let topics = BTreeMap::from([("orders".to_owned(), 3)]);

    let args = operation_args("127.0.0.1:19091,127.0.0.1:19092", &topics, 2);

    assert_eq!(
        args,
        [
            "--bootstrap-server",
            "127.0.0.1:19091,127.0.0.1:19092",
            "--readiness-topic",
            "testlab-environment-readiness",
            "--topic",
            "orders",
            "--partitions",
            "3",
            "--replication-factor",
            "2",
        ]
    );
}
