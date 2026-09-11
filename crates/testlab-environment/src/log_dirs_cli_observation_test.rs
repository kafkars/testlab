use testlab_schema::{BrokerStateObservation, OperationId};

use super::*;

const OUTPUT: &str = "Querying brokers for log directories information\nReceived log directory information from brokers 2,1\n{\"version\":1,\"brokers\":[{\"broker\":2,\"logDirs\":[{\"logDir\":\"/data-b\",\"error\":null,\"partitions\":[]}]},{\"broker\":1,\"logDirs\":[{\"logDir\":\"/data-a\",\"error\":null,\"partitions\":[{\"partition\":\"orders-0\",\"size\":42,\"offsetLag\":-1,\"isFuture\":false}]}]}]}\n";

#[test]
fn json_is_parsed_exactly_and_canonicalized() {
    let observed = normalize(7, &target(), OUTPUT.as_bytes())
        .unwrap_or_else(|error| panic!("normalize log-directory state: {error}"));
    let BrokerStateObservation::LogDirs(observed) = observed else {
        panic!("log-directory observation kind");
    };
    assert_eq!(observed.observation, 7);
    assert_eq!(observed.brokers[0].broker_id, 1);
    assert_eq!(observed.brokers[0].log_dirs[0].replicas[0].size_bytes, 42);
    assert_eq!(observed.brokers[0].log_dirs[0].replicas[0].offset_lag, -1);
    assert_eq!(observed.brokers[1].broker_id, 2);
}

#[test]
fn bad_status_version_errors_and_foreign_partitions_fail_closed() {
    assert!(normalize(1, &target(), b"{}\n").is_err());
    assert!(
        normalize(
            1,
            &target(),
            OUTPUT.replace("\"version\":1", "\"version\":2").as_bytes()
        )
        .is_err()
    );
    assert!(
        normalize(
            1,
            &target(),
            OUTPUT
                .replace("\"error\":null", "\"error\":\"failure\"")
                .as_bytes()
        )
        .is_err()
    );
    assert!(
        normalize(
            1,
            &target(),
            OUTPUT.replace("orders-0", "orders-1").as_bytes()
        )
        .is_err()
    );
}

fn target() -> LogDirsTarget {
    LogDirsTarget {
        operation_id: OperationId::new("admin-log-dirs")
            .unwrap_or_else(|error| panic!("operation ID: {error}")),
        topic: "orders".to_owned(),
        partition: 0,
    }
}
