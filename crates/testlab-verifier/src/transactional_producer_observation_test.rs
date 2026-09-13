//! Transactional producer observation tests pin public owner identity and activity.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, BatchRecord, ClientId, CommandEnvelope,
    CommandId, HistoryEntry, HistoryPayload, OperationId, ProducerId, Scenario, ScenarioAction,
    TerminalStatus, TransactionFenceMethod, TransactionalProducerObservation,
    VisibilityExpectation,
};

use super::verify;
use crate::index::HistoryIndex;
use crate::verify_fixture::{record, scenario, step};

#[test]
fn creation_requires_exact_nonnegative_active_public_identity() {
    let scenario = creation_scenario();
    let exact = observation(producer("transactional-1"), "testlab-transaction", 41, 2);
    assert!(violations(&scenario, creation_history(exact.clone(), 2)).is_empty());

    let mut wrong_transactional_id = exact.clone();
    wrong_transactional_id.transactional_id = "other".to_owned();
    assert_contract(
        &violations(&scenario, creation_history(wrong_transactional_id, 2)),
        "TXN-013",
    );

    let mut negative_id = exact.clone();
    negative_id.kafka_producer_id = -1;
    assert_contract(
        &violations(&scenario, creation_history(negative_id, 2)),
        "TXN-013",
    );

    let mut negative_epoch = exact.clone();
    negative_epoch.kafka_producer_epoch = -1;
    assert_contract(
        &violations(&scenario, creation_history(negative_epoch, 2)),
        "TXN-013",
    );

    let mut inactive = exact;
    inactive.active = false;
    assert_contract(
        &violations(&scenario, creation_history(inactive, 2)),
        "TXN-013",
    );
}

#[test]
fn creation_observation_must_be_unique_and_follow_its_command() {
    let scenario = creation_scenario();
    let exact = observation(producer("transactional-1"), "testlab-transaction", 41, 2);
    assert_contract(
        &violations(&scenario, creation_history(exact.clone(), 0)),
        "TXN-013",
    );
    let mut duplicate = creation_history(exact.clone(), 2);
    duplicate.push(event(
        3,
        "create",
        AdapterEvent::TransactionalProducerCreated(exact),
    ));
    assert_contract(&violations(&scenario, duplicate), "TXN-013");
}

#[test]
fn identical_denied_then_recovered_commands_bind_only_the_success() {
    let mut scenario = creation_scenario();
    scenario.steps = vec![
        step(
            "denied",
            creation_action(Some("transactional_id_authorization".to_owned())),
        ),
        step("recovered", creation_action(None)),
    ];
    let observation = observation(producer("transactional-1"), "testlab-transaction", 41, 2);
    let history = vec![
        command_entry(1, "denied", creation_command()),
        command_entry(2, "recovered", creation_command()),
        event(
            3,
            "recovered",
            AdapterEvent::TransactionalProducerCreated(observation),
        ),
    ];
    assert!(violations(&scenario, history).is_empty());
}

#[test]
fn fencing_observes_the_replacement_public_owner() {
    let scenario = fence_scenario();
    let command = fence_command();
    let exact = observation(producer("replacement"), "testlab-fence", 41, 3);
    let history = vec![
        command_entry(1, "fence", command.clone()),
        event(
            2,
            "fence",
            AdapterEvent::TransactionalProducerCreated(exact.clone()),
        ),
    ];
    assert!(violations(&scenario, history).is_empty());

    let mut wrong = exact;
    wrong.transactional_id = "other".to_owned();
    let history = vec![
        command_entry(1, "fence", command),
        event(
            2,
            "fence",
            AdapterEvent::TransactionalProducerCreated(wrong),
        ),
    ];
    assert_contract(&violations(&scenario, history), "TXN-013");
}

fn creation_scenario() -> Scenario {
    let mut value = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    value.steps = vec![step("transactional-producer", creation_action(None))];
    value.assertions.clear();
    value
}

fn creation_history(
    observation: TransactionalProducerObservation,
    event_sequence: u64,
) -> Vec<HistoryEntry> {
    vec![
        command_entry(1, "create", creation_command()),
        event(
            event_sequence,
            "create",
            AdapterEvent::TransactionalProducerCreated(observation),
        ),
    ]
}

fn creation_action(expected_error_code: Option<String>) -> ScenarioAction {
    ScenarioAction::CreateTransactionalProducer {
        client_id: client("client-1"),
        producer_id: producer("transactional-1"),
        transactional_id: "testlab-transaction".to_owned(),
        transaction_timeout_ms: 30_000,
        initialization_timeout_ms: 5_000,
        expected_error_code,
    }
}

fn creation_command() -> AdapterCommand {
    AdapterCommand::CreateTransactionalProducer {
        client_id: client("client-1"),
        producer_id: producer("transactional-1"),
        transactional_id: "testlab-transaction".to_owned(),
        transaction_timeout_ms: 30_000,
        initialization_timeout_ms: 5_000,
    }
}

fn fence_scenario() -> Scenario {
    let mut value = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    value.steps = vec![step(
        "fence",
        ScenarioAction::FenceTransaction {
            fence_method: TransactionFenceMethod::ReplacementInitialization,
            producer_id: producer("original"),
            transaction_id: operation("fenced-transaction"),
            operation: batch_record(),
            replacement_client_id: client("client-1"),
            replacement_producer_id: producer("replacement"),
            transactional_id: "testlab-fence".to_owned(),
            transaction_timeout_ms: 30_000,
            initialization_timeout_ms: 5_000,
            timeout_ms: 10_000,
        },
    )];
    value.assertions.clear();
    value
}

fn fence_command() -> AdapterCommand {
    AdapterCommand::FenceTransaction {
        fence_method: TransactionFenceMethod::ReplacementInitialization,
        producer_id: producer("original"),
        transaction_id: operation("fenced-transaction"),
        operation: batch_record(),
        replacement_client_id: client("client-1"),
        replacement_producer_id: producer("replacement"),
        transactional_id: "testlab-fence".to_owned(),
        transaction_timeout_ms: 30_000,
        initialization_timeout_ms: 5_000,
        timeout_ms: 10_000,
    }
}

fn batch_record() -> BatchRecord {
    BatchRecord {
        operation_id: operation("record-1"),
        record: record("value"),
    }
}

fn observation(
    producer_id: ProducerId,
    transactional_id: &str,
    kafka_producer_id: i64,
    kafka_producer_epoch: i16,
) -> TransactionalProducerObservation {
    TransactionalProducerObservation {
        producer_id,
        transactional_id: transactional_id.to_owned(),
        kafka_producer_id,
        kafka_producer_epoch,
        active: true,
    }
}

fn command_entry(sequence: u64, id: &str, command: AdapterCommand) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::HarnessCommand {
            command: CommandEnvelope::new(command_id(id), command),
        },
    }
}

fn event(sequence: u64, id: &str, event: AdapterEvent) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::AdapterEvent {
            event: AdapterEventEnvelope::new(command_id(id), event),
        },
    }
}

fn client(value: &str) -> ClientId {
    ClientId::new(value).unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn producer(value: &str) -> ProducerId {
    ProducerId::new(value).unwrap_or_else(|error| panic!("producer ID: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}

fn command_id(value: &str) -> CommandId {
    CommandId::new(value).unwrap_or_else(|error| panic!("command ID: {error}"))
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the test helper owns each constructed history fixture"
)]
fn violations(scenario: &Scenario, history: Vec<HistoryEntry>) -> Vec<testlab_schema::Violation> {
    let mut violations = Vec::new();
    verify(scenario, &HistoryIndex::build(&history), &mut violations);
    violations
}

fn assert_contract(violations: &[testlab_schema::Violation], contract_id: &str) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract_id),
        "{violations:?}"
    );
}
