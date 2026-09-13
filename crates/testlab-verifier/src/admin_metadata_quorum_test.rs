use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminMetadataQuorumDescription, BrokerMetadataQuorumState,
    BrokerStateObservation, DescribeMetadataQuorumAction, DescribeMetadataQuorumCommand,
    HistoryEntry, HistoryPayload, MetadataQuorumListenerState, MetadataQuorumNodeState,
    MetadataQuorumReplicaState, OperationId, ScenarioAction, TerminalStatus, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn canonical_public_quorum_may_progress_to_the_immediate_cli_snapshot() {
    let mut history = history();
    let HistoryPayload::AdapterEvent { event } = &mut history[1].payload else {
        panic!("public quorum event");
    };
    let AdapterEvent::MetadataQuorumDescribed(state) = &mut event.event else {
        panic!("quorum event kind");
    };
    state.voters[0].last_fetch_timestamp_ms = None;
    assert!(violations(history).is_empty());
}

#[test]
fn leader_directory_regression_and_timing_mismatches_fail() {
    let mut leader = history();
    let HistoryPayload::BrokerStateObservation { observation } = &mut leader[2].payload else {
        panic!("quorum state");
    };
    let BrokerStateObservation::MetadataQuorum(state) = observation else {
        panic!("quorum observation kind");
    };
    state.leader_id = Some(2);
    assert_contract(&violations(leader));

    let mut directory = history();
    let HistoryPayload::AdapterEvent { event } = &mut directory[1].payload else {
        panic!("public quorum event");
    };
    let AdapterEvent::MetadataQuorumDescribed(state) = &mut event.event else {
        panic!("quorum event kind");
    };
    state.voters[0].replica_directory_id = None;
    assert_contract(&violations(directory));

    let mut regressed = history();
    let HistoryPayload::BrokerStateObservation { observation } = &mut regressed[2].payload else {
        panic!("quorum state");
    };
    let BrokerStateObservation::MetadataQuorum(state) = observation else {
        panic!("quorum observation kind");
    };
    state.voters[0].log_end_offset = Some(39);
    assert_contract(&violations(regressed));

    let mut lost_known_state = history();
    let HistoryPayload::BrokerStateObservation { observation } = &mut lost_known_state[2].payload
    else {
        panic!("quorum state");
    };
    let BrokerStateObservation::MetadataQuorum(state) = observation else {
        panic!("quorum observation kind");
    };
    state.voters[0].last_fetch_timestamp_ms = None;
    assert_contract(&violations(lost_known_state));

    let mut delayed = history();
    delayed.insert(2, command(2, AdapterCommand::Finish));
    delayed[3].sequence = 3;
    delayed[3].observed_unix_ms = 3;
    assert_contract(&violations(delayed));
}

fn history() -> Vec<HistoryEntry> {
    vec![
        command(
            0,
            AdapterCommand::DescribeMetadataQuorum(DescribeMetadataQuorumCommand {
                client_id: client(),
                operation_id: operation(),
                timeout_ms: 1_000,
            }),
        ),
        event(
            1,
            AdapterEvent::MetadataQuorumDescribed(AdminMetadataQuorumDescription {
                operation_id: operation(),
                leader_id: Some(1),
                leader_epoch: 7,
                high_watermark: 39,
                voters: vec![replica(40, 100)],
                observers: Vec::new(),
                nodes: Some(vec![node()]),
            }),
        ),
        state(
            2,
            BrokerStateObservation::MetadataQuorum(BrokerMetadataQuorumState {
                observation: 4,
                operation_id: operation(),
                leader_id: Some(1),
                leader_epoch: 7,
                high_watermark: 40,
                voters: vec![replica(42, 102)],
                observers: Vec::new(),
                nodes: vec![node()],
            }),
        ),
    ]
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the test helper owns each constructed history fixture"
)]
fn violations(history: Vec<HistoryEntry>) -> Vec<testlab_schema::Violation> {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.steps.insert(
        2,
        step(
            "describe-metadata-quorum",
            ScenarioAction::DescribeMetadataQuorum(DescribeMetadataQuorumAction {
                client_id: client(),
                operation_id: operation(),
                timeout_ms: 1_000,
            }),
        ),
    );
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    verify_admin(&scenario, &index, &[], &mut violations);
    violations
}

fn replica(offset: i64, timestamp: i64) -> MetadataQuorumReplicaState {
    MetadataQuorumReplicaState {
        replica_id: 1,
        replica_directory_id: Some("AAECAwQFBgcICQoLDA0ODw".to_owned()),
        log_end_offset: Some(offset),
        last_fetch_timestamp_ms: Some(timestamp),
        last_caught_up_timestamp_ms: Some(timestamp),
    }
}

fn node() -> MetadataQuorumNodeState {
    MetadataQuorumNodeState {
        node_id: 1,
        listeners: vec![MetadataQuorumListenerState {
            name: "CONTROLLER".to_owned(),
            host: "broker-1".to_owned(),
            port: 9093,
        }],
    }
}

fn state(sequence: u64, observation: BrokerStateObservation) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation { observation },
    }
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("describe-metadata-quorum")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-056"),
        "{violations:?}"
    );
}
