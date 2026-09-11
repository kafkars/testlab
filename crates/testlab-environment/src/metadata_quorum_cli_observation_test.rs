use testlab_schema::BrokerStateObservation;

use super::normalize;

const STATUS: &str = r#"ClusterId:              AAECAwQFBgcICQoLDA0ODw
LeaderId:               2
LeaderEpoch:            7
HighWatermark:          40
MaxFollowerLag:         2
MaxFollowerLagTimeMs:   9
CurrentVoters:          [{"id": 2, "directoryId": "AgICAgICAgICAgICAgICAg", "endpoints": ["CONTROLLER://broker-2:9093"]}, {"id": 1, "directoryId": "AQEBAQEBAQEBAQEBAQEBAQ", "endpoints": ["CONTROLLER://broker-1:9093"]}]
CurrentObservers:       [{"id": 3, "directoryId": null}]
"#;

const REPLICATION: &str = "\
NodeId DirectoryId LogEndOffset Lag LastFetchTimestamp LastCaughtUpTimestamp Status\n\
2 AgICAgICAgICAgICAgICAg 42 0 -1 -1 Leader\n\
1 AQEBAQEBAQEBAQEBAQEBAQ 40 2 100 101 Follower\n\
3 AAAAAAAAAAAAAAAAAAAAAA 39 3 102 103 Observer\n";

#[test]
fn status_and_replication_views_join_into_canonical_state() {
    let observed = normalize(5, &operation(), STATUS.as_bytes(), REPLICATION.as_bytes())
        .unwrap_or_else(|error| panic!("normalize quorum: {error}"));
    let BrokerStateObservation::MetadataQuorum(observed) = observed else {
        panic!("quorum observation kind");
    };
    assert_eq!(observed.observation, 5);
    assert_eq!(observed.leader_id, Some(2));
    assert_eq!(observed.voters[0].replica_id, 1);
    assert_eq!(observed.voters[1].replica_id, 2);
    assert_eq!(observed.observers[0].replica_directory_id, None);
    assert_eq!(observed.nodes[0].listeners[0].host, "broker-1");
}

#[test]
fn mismatched_role_directory_and_lag_fail_closed() {
    let follower_leader = REPLICATION.replace("Follower", "Leader");
    assert!(
        normalize(
            1,
            &operation(),
            STATUS.as_bytes(),
            follower_leader.as_bytes()
        )
        .is_err()
    );
    let wrong_directory = REPLICATION.replace("AQEBAQEBAQEBAQEBAQEBAQ", "AwMDAwMDAwMDAwMDAwMDAw");
    assert!(
        normalize(
            1,
            &operation(),
            STATUS.as_bytes(),
            wrong_directory.as_bytes()
        )
        .is_err()
    );
    let wrong_lag = STATUS.replace("MaxFollowerLag:         2", "MaxFollowerLag:         3");
    assert!(
        normalize(
            1,
            &operation(),
            wrong_lag.as_bytes(),
            REPLICATION.as_bytes()
        )
        .is_err()
    );
}

#[test]
fn bracketed_ipv6_is_normalized_and_nested_brackets_fail_closed() {
    let ipv6 = STATUS.replace("broker-2", "[::1]");
    let observed = normalize(1, &operation(), ipv6.as_bytes(), REPLICATION.as_bytes())
        .unwrap_or_else(|error| panic!("normalize IPv6 quorum: {error}"));
    let BrokerStateObservation::MetadataQuorum(observed) = observed else {
        panic!("quorum observation kind");
    };
    assert_eq!(observed.nodes[1].listeners[0].host, "::1");

    let nested = STATUS.replace("broker-2", "[[::1]");
    assert!(normalize(1, &operation(), nested.as_bytes(), REPLICATION.as_bytes()).is_err());
}

fn operation() -> testlab_schema::OperationId {
    testlab_schema::OperationId::new("describe-metadata-quorum")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
