//! Scenario validation owns identity, lifecycle, and assertion coherence.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    Capability, ClientId, OperationId, ProducerId, SCENARIO_SCHEMA_VERSION, Scenario,
    ScenarioAction, ScenarioError, TerminalStatus, VisibilityExpectation,
};

type ClientStates = BTreeMap<ClientId, bool>;
type ProducerStates = BTreeMap<ProducerId, (ClientId, bool)>;

pub(crate) fn validate(scenario: &Scenario) -> Result<(), ScenarioError> {
    let mut problems = Vec::new();
    validate_header(scenario, &mut problems);
    validate_steps(scenario, &mut problems);
    if problems.is_empty() {
        Ok(())
    } else {
        Err(ScenarioError { problems })
    }
}

fn validate_header(scenario: &Scenario, problems: &mut Vec<String>) {
    if scenario.schema_version != SCENARIO_SCHEMA_VERSION {
        problems.push(format!(
            "unsupported schema version {}, expected {SCENARIO_SCHEMA_VERSION}",
            scenario.schema_version
        ));
    }
    if scenario.title.trim().is_empty() {
        problems.push("title must not be empty".to_owned());
    }
    if scenario.description.trim().is_empty() {
        problems.push("description must not be empty".to_owned());
    }
    if !(100..=600_000).contains(&scenario.timeout_ms) {
        problems.push("timeout_ms must be between 100 and 600000".to_owned());
    }
    if scenario.steps.is_empty() {
        problems.push("scenario must contain at least one step".to_owned());
    }
}

fn validate_steps(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut step_ids = BTreeSet::new();
    let mut clients = ClientStates::new();
    let mut producers = ProducerStates::new();
    let mut operations = BTreeSet::new();
    let mut uses_model_broker = false;
    for step in &scenario.steps {
        if !step_ids.insert(step.id.clone()) {
            problems.push(format!("duplicate step id {}", step.id));
        }
        uses_model_broker |= matches!(&step.action, ScenarioAction::SetBrokerBehavior { .. });
        validate_action(
            &step.action,
            &mut clients,
            &mut producers,
            &mut operations,
            problems,
        );
    }
    if !operations.is_empty() && !scenario.requires.contains(&Capability::Producer) {
        problems.push("send steps require the producer capability".to_owned());
    }
    if uses_model_broker && !scenario.requires.contains(&Capability::ModelBroker) {
        problems.push("broker-control steps require the model_broker capability".to_owned());
    }
    if (!clients.is_empty() || !producers.is_empty())
        && !scenario.requires.contains(&Capability::Lifecycle)
    {
        problems.push("handle steps require the lifecycle capability".to_owned());
    }
    validate_assertions(scenario, &operations, problems);
    for (producer, (_, closed)) in producers {
        if !closed {
            problems.push(format!("producer {producer} was not closed"));
        }
    }
    for (client, shutdown) in clients {
        if !shutdown {
            problems.push(format!("client {client} was not shut down"));
        }
    }
}

fn validate_action(
    action: &ScenarioAction,
    clients: &mut ClientStates,
    producers: &mut ProducerStates,
    operations: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    match action {
        ScenarioAction::CreateClient { client_id } => {
            if clients.insert(client_id.clone(), false).is_some() {
                problems.push(format!("duplicate client id {client_id}"));
            }
        }
        ScenarioAction::CreateProducer {
            client_id,
            producer_id,
        } => create_producer(client_id, producer_id, clients, producers, problems),
        ScenarioAction::SetBrokerBehavior { .. } => {}
        ScenarioAction::Send {
            producer_id,
            operation_id,
            record,
        } => {
            require_open_producer(producer_id, producers, problems);
            if !operations.insert(operation_id.clone()) {
                problems.push(format!("duplicate operation id {operation_id}"));
            }
            if let Err(error) = record.validate() {
                problems.push(format!(
                    "operation {operation_id} has invalid record: {error}"
                ));
            }
        }
        ScenarioAction::Flush { producer_id } => {
            require_open_producer(producer_id, producers, problems);
        }
        ScenarioAction::CloseProducer { producer_id } => {
            close_producer(producer_id, producers, problems);
        }
        ScenarioAction::ShutdownClient { client_id } => {
            shutdown_client(client_id, clients, producers, problems);
        }
    }
}

fn create_producer(
    client_id: &ClientId,
    producer_id: &ProducerId,
    clients: &ClientStates,
    producers: &mut ProducerStates,
    problems: &mut Vec<String>,
) {
    match clients.get(client_id) {
        Some(false) => {}
        Some(true) => problems.push(format!(
            "producer {producer_id} uses shut down client {client_id}"
        )),
        None => problems.push(format!(
            "producer {producer_id} uses missing client {client_id}"
        )),
    }
    if producers
        .insert(producer_id.clone(), (client_id.clone(), false))
        .is_some()
    {
        problems.push(format!("duplicate producer id {producer_id}"));
    }
}

fn require_open_producer(
    producer_id: &ProducerId,
    producers: &ProducerStates,
    problems: &mut Vec<String>,
) {
    match producers.get(producer_id) {
        Some((_, false)) => {}
        Some((_, true)) => {
            problems.push(format!("producer {producer_id} was used after close"));
        }
        None => problems.push(format!("missing producer {producer_id} was used")),
    }
}

fn close_producer(
    producer_id: &ProducerId,
    producers: &mut ProducerStates,
    problems: &mut Vec<String>,
) {
    match producers.get_mut(producer_id) {
        Some((_, closed)) if !*closed => *closed = true,
        Some(_) => {
            problems.push(format!("producer {producer_id} closed more than once"));
        }
        None => problems.push(format!("missing producer {producer_id} was closed")),
    }
}

fn shutdown_client(
    client_id: &ClientId,
    clients: &mut ClientStates,
    producers: &ProducerStates,
    problems: &mut Vec<String>,
) {
    let open = producers
        .iter()
        .filter(|(_, (owner, closed))| owner == client_id && !closed)
        .map(|(producer, _)| producer.to_string())
        .collect::<Vec<_>>();
    if !open.is_empty() {
        problems.push(format!(
            "client {client_id} shut down with open producers {}",
            open.join(", ")
        ));
    }
    match clients.get_mut(client_id) {
        Some(shutdown) if !*shutdown => *shutdown = true,
        Some(_) => {
            problems.push(format!("client {client_id} shut down more than once"));
        }
        None => problems.push(format!("missing client {client_id} was shut down")),
    }
}

fn validate_assertions(
    scenario: &Scenario,
    operations: &BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    let mut asserted = BTreeSet::new();
    for assertion in &scenario.assertions {
        if !operations.contains(&assertion.operation_id) {
            problems.push(format!(
                "assertion references missing operation {}",
                assertion.operation_id
            ));
        }
        if !asserted.insert(assertion.operation_id.clone()) {
            problems.push(format!(
                "duplicate assertion for operation {}",
                assertion.operation_id
            ));
        }
        validate_assertion_semantics(assertion, problems);
    }
    for operation in operations {
        if !asserted.contains(operation) {
            problems.push(format!("operation {operation} has no assertion"));
        }
    }
}

fn validate_assertion_semantics(assertion: &crate::OperationAssertion, problems: &mut Vec<String>) {
    match (assertion.accepted, assertion.terminal) {
        (true, None) => problems.push(format!(
            "accepted operation {} requires a terminal expectation",
            assertion.operation_id
        )),
        (false, Some(_)) => problems.push(format!(
            "rejected operation {} must not declare a terminal expectation",
            assertion.operation_id
        )),
        _ => {}
    }
    if !assertion.accepted && assertion.visibility != VisibilityExpectation::Absent {
        problems.push(format!(
            "rejected operation {} must expect absent visibility",
            assertion.operation_id
        ));
    }
    if assertion.terminal == Some(TerminalStatus::Acknowledged)
        && assertion.visibility != VisibilityExpectation::ExactlyOnce
    {
        problems.push(format!(
            "acknowledged operation {} must expect exactly-once visibility",
            assertion.operation_id
        ));
    }
    if assertion.terminal == Some(TerminalStatus::DefinitelyNotSent)
        && assertion.visibility != VisibilityExpectation::Absent
    {
        problems.push(format!(
            "definitely-not-sent operation {} must expect absent visibility",
            assertion.operation_id
        ));
    }
}
