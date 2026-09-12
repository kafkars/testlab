//! Automatic producer partition tests distinguish command, receipt, and broker mismatches.

use testlab_schema::{
    AdapterCommand, AdapterEvent, ByteString, HistoryEntry, ProducerPartitioning, ScenarioAction,
    TerminalStatus, VisibilityExpectation,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, observation, scenario};

#[test]
fn matching_java_keyed_command_receipt_and_broker_record_pass() {
    let (scenario, history, observed) = fixture();

    let violations = verify_partitioning(&scenario, &history, &observed);

    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn explicit_command_for_automatic_scenario_fails() {
    let (scenario, mut history, observed) = fixture();
    let testlab_schema::HistoryPayload::HarnessCommand { command } = &mut history[0].payload else {
        panic!("fixture command missing");
    };
    let AdapterCommand::Send { partitioning, .. } = &mut command.command else {
        panic!("fixture send command missing");
    };
    *partitioning = ProducerPartitioning::Explicit;

    let violations = verify_partitioning(&scenario, &history, &observed);

    assert!(violates(&violations, "PROD-015"));
}

#[test]
fn public_or_broker_partition_mismatch_fails() {
    let (scenario, mut history, mut observed) = fixture();
    set_terminal_partition(&mut history, 1);

    let public_violations = verify_partitioning(&scenario, &history, &observed);
    assert!(violates(&public_violations, "PROD-015"));

    set_terminal_partition(&mut history, 2);
    observed[0].record.partition = 1;
    observed[0].digest = observed[0]
        .record
        .digest()
        .unwrap_or_else(|error| panic!("record digest: {error}"));
    let broker_violations = verify_partitioning(&scenario, &history, &observed);
    assert!(violates(&broker_violations, "PROD-015"));
}

fn set_terminal_partition(history: &mut [HistoryEntry], value: i32) {
    let testlab_schema::HistoryPayload::AdapterEvent { event } = &mut history[1].payload else {
        panic!("fixture terminal missing");
    };
    let AdapterEvent::OperationTerminal { partition, .. } = &mut event.event else {
        panic!("fixture terminal event missing");
    };
    *partition = Some(value);
}

fn fixture() -> (
    testlab_schema::Scenario,
    Vec<HistoryEntry>,
    Vec<testlab_schema::BrokerObservation>,
) {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    let (producer_id, operation_id, partitioning, record) = scenario
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::Send {
                producer_id,
                operation_id,
                partitioning,
                record,
                ..
            } => {
                record.partition = 2;
                record.key = Some(ByteString::utf8("kafkars"));
                *partitioning = ProducerPartitioning::JavaKeyed { partition_count: 3 };
                Some((
                    producer_id.clone(),
                    operation_id.clone(),
                    *partitioning,
                    record.clone(),
                ))
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("fixture scenario omitted send"));
    let history = vec![
        command(
            0,
            AdapterCommand::Send {
                producer_id,
                operation_id: operation_id.clone(),
                method: Default::default(),
                partitioning,
                record,
            },
        ),
        event(
            1,
            AdapterEvent::OperationTerminal {
                operation_id,
                status: TerminalStatus::Acknowledged,
                code: None,
                partition: Some(2),
                offset: Some(0),
                timestamp_millis: None,
            },
        ),
    ];
    let mut observed = observation(0, "value");
    observed.record.partition = 2;
    observed.record.key = Some(ByteString::utf8("kafkars"));
    observed.digest = observed
        .record
        .digest()
        .unwrap_or_else(|error| panic!("record digest: {error}"));
    (scenario, history, vec![observed])
}

fn verify_partitioning(
    scenario: &testlab_schema::Scenario,
    history: &[HistoryEntry],
    observations: &[testlab_schema::BrokerObservation],
) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    crate::producer_partitioning::verify(scenario, &index, observations, &mut violations);
    violations
}

fn violates(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
