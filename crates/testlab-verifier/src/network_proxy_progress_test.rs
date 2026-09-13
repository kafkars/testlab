//! Network recovery tests pin public consumer progress after exact controls.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterEvent, ByteString, ConsumedRecord, ConsumerId, EnvironmentOperationId,
    GroupMembershipEpoch, HistoryEntry, HistoryPayload, NetworkConnectionCutAction,
    NetworkProxyControl, OperationId, Scenario, ScenarioAction, ScenarioId, ShareConsumedRecord,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, step};

#[test]
fn exact_committed_group_progress_after_connection_cut_passes() {
    let index = HistoryIndex::build(&history(true, true));
    let violations = violations(&index);
    assert!(!has(&violations, "NET-005"), "{violations:?}");
}

#[test]
fn missing_group_completion_after_connection_cut_fails() {
    let index = HistoryIndex::build(&history(false, true));
    let violations = violations(&index);
    assert!(has(&violations, "NET-005"), "{violations:?}");
}

#[test]
fn uncommitted_group_progress_after_connection_cut_fails() {
    let index = HistoryIndex::build(&history(true, false));
    let violations = violations(&index);
    assert!(has(&violations, "NET-005"), "{violations:?}");
}

#[test]
fn assigned_and_share_progress_after_connection_cut_pass() {
    for (scenario, history) in [
        (assigned_scenario(), assigned_history()),
        (share_scenario(), share_history()),
    ] {
        let index = HistoryIndex::build(&history);
        let mut violations = Vec::new();
        super::consumer_progress::verify(&scenario, &index, &mut violations);
        assert!(!has(&violations, "NET-005"), "{violations:?}");
    }
}

fn violations(index: &HistoryIndex) -> Vec<testlab_schema::Violation> {
    let mut violations = Vec::new();
    super::consumer_progress::verify(&scenario(), index, &mut violations);
    violations
}

fn scenario() -> Scenario {
    recovery_scenario(ScenarioAction::GroupReceive {
        consumer_id: consumer(),
        method: Default::default(),
        checkpoint_method: Default::default(),
        receive_id: operation("receive-after-cut"),
        expected_operation_id: operation("send-after-cut"),
        additional_expected_operation_ids: Vec::new(),
        processed_record_count: None,
        processing_acknowledgement_delay_ms: 0,
        expected_error_code: None,
        timeout_ms: 1_000,
    })
}

fn assigned_scenario() -> Scenario {
    recovery_scenario(ScenarioAction::Receive {
        consumer_id: consumer(),
        method: Default::default(),
        observe_fetch_evidence: false,
        receive_id: operation("assigned-after-cut"),
        expected_operation_id: operation("send-after-cut"),
        timeout_ms: 1_000,
    })
}

fn share_scenario() -> Scenario {
    recovery_scenario(ScenarioAction::ShareReceive {
        consumer_id: consumer(),
        receive_id: operation("share-after-cut"),
        expected_operation_ids: vec![operation("send-after-cut")],
        minimum_delivery_count: 1,
        expected_acquisition_count: None,
        timeout_ms: 1_000,
    })
}

fn recovery_scenario(action: ScenarioAction) -> Scenario {
    Scenario {
        schema_version: testlab_schema::SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("network.group-recovery")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "group recovery".to_owned(),
        description: "network group recovery fixture".to_owned(),
        timeout_ms: 10_000,
        requires: BTreeSet::new(),
        steps: vec![
            step(
                "cut",
                ScenarioAction::CutNetworkConnections(NetworkConnectionCutAction {
                    operation_id: environment_id("cut-connections"),
                    broker_ordinal: 1,
                    timeout_ms: 1_000,
                }),
            ),
            step("receive", action),
        ],
        assertions: Vec::new(),
    }
}

fn history(include_completion: bool, committed: bool) -> Vec<HistoryEntry> {
    let mut history = recovery_history(AdapterCommand::GroupReceive {
        consumer_id: consumer(),
        method: Default::default(),
        checkpoint_method: Default::default(),
        receive_id: operation("receive-after-cut"),
        processing_acknowledgement_delay_ms: 0,
        processed_record_count: None,
        timeout_ms: 1_000,
    });
    if include_completion {
        history.push(event(
            2,
            AdapterEvent::GroupReceiveCompleted {
                receive_id: operation("receive-after-cut"),
                records: vec![ConsumedRecord {
                    topic: "records".to_owned(),
                    partition: 0,
                    offset: 1,
                    timestamp_millis: None,
                    key: None,
                    value: Some(ByteString::utf8("after-cut")),
                    headers: Vec::new(),
                }],
                committed,
                group_epoch: Some(GroupMembershipEpoch::Classic { generation_id: 1 }),
            },
        ));
    }
    history
}

fn assigned_history() -> Vec<HistoryEntry> {
    let mut history = recovery_history(AdapterCommand::Receive {
        consumer_id: consumer(),
        method: Default::default(),
        receive_id: operation("assigned-after-cut"),
        timeout_ms: 1_000,
    });
    history.push(event(
        2,
        AdapterEvent::ReceiveCompleted {
            receive_id: operation("assigned-after-cut"),
            records: vec![consumed()],
            fetch_evidence: None,
        },
    ));
    history
}

fn share_history() -> Vec<HistoryEntry> {
    let mut history = recovery_history(AdapterCommand::ShareReceive {
        consumer_id: consumer(),
        receive_id: operation("share-after-cut"),
        timeout_ms: 1_000,
    });
    history.push(event(
        2,
        AdapterEvent::ShareReceiveCompleted {
            consumer_id: consumer(),
            receive_id: operation("share-after-cut"),
            records: vec![ShareConsumedRecord {
                record: consumed(),
                delivery_count: 1,
            }],
            acquisition_count: 1,
            member_epoch: Some(1),
            assignment_epoch: Some(1),
        },
    ));
    history
}

fn recovery_history(receive: AdapterCommand) -> Vec<HistoryEntry> {
    let cut = NetworkProxyControl::CutConnections(NetworkConnectionCutAction {
        operation_id: environment_id("cut-connections"),
        broker_ordinal: 1,
        timeout_ms: 1_000,
    });
    vec![
        HistoryEntry {
            sequence: 0,
            observed_unix_ms: 0,
            payload: HistoryPayload::NetworkProxyControl { control: cut },
        },
        command(1, receive),
    ]
}

fn consumed() -> ConsumedRecord {
    ConsumedRecord {
        topic: "records".to_owned(),
        partition: 0,
        offset: 1,
        timestamp_millis: None,
        key: None,
        value: Some(ByteString::utf8("after-cut")),
        headers: Vec::new(),
    }
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn environment_id(value: &str) -> EnvironmentOperationId {
    EnvironmentOperationId::new(value)
        .unwrap_or_else(|error| panic!("environment operation id: {error}"))
}

fn consumer() -> ConsumerId {
    ConsumerId::new("consumer-1").unwrap_or_else(|error| panic!("consumer id: {error}"))
}

fn has(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
