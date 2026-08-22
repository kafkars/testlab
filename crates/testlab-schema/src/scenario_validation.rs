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
    let mut consumers = crate::consumer_action_validation::ConsumerStates::new();
    let mut operation_ids = BTreeSet::new();
    let mut sends = BTreeSet::new();
    let mut uses_model_broker = false;
    let mut uses_readiness = false;
    let mut uses_batch = false;
    let mut uses_consumer = false;
    for step in &scenario.steps {
        if !step_ids.insert(step.id.clone()) {
            problems.push(format!("duplicate step id {}", step.id));
        }
        uses_model_broker |= matches!(&step.action, ScenarioAction::SetBrokerBehavior { .. });
        uses_readiness |= matches!(&step.action, ScenarioAction::AwaitClientReady { .. });
        uses_batch |= matches!(&step.action, ScenarioAction::SendBatch { .. });
        uses_consumer |= matches!(
            &step.action,
            ScenarioAction::CreateAssignedConsumer { .. }
                | ScenarioAction::AssignBeginning { .. }
                | ScenarioAction::Receive { .. }
                | ScenarioAction::CloseAssignedConsumer { .. }
        );
        crate::scenario_action_validation::validate_action(
            &step.action,
            &mut clients,
            &mut producers,
            &mut consumers,
            &mut operation_ids,
            &mut sends,
            problems,
        );
    }
    if !sends.is_empty() && !scenario.requires.contains(&Capability::Producer) {
        problems.push("send steps require the producer capability".to_owned());
    }
    if uses_batch && !scenario.requires.contains(&Capability::ProducerBatch) {
        problems.push("batch-send steps require the producer_batch capability".to_owned());
    }
    if uses_consumer && !scenario.requires.contains(&Capability::AssignedConsumer) {
        problems
            .push("assigned-consumer steps require the assigned_consumer capability".to_owned());
    }
    if uses_model_broker && !scenario.requires.contains(&Capability::ModelBroker) {
        problems.push("broker-control steps require the model_broker capability".to_owned());
    }
    if uses_readiness && !scenario.requires.contains(&Capability::ClientReadiness) {
        problems.push("client readiness steps require the client_readiness capability".to_owned());
    }
    if (!clients.is_empty() || !producers.is_empty() || !consumers.is_empty())
        && !scenario.requires.contains(&Capability::Lifecycle)
    {
        problems.push("handle steps require the lifecycle capability".to_owned());
    }
    validate_assertions(scenario, &sends, problems);
    for (producer, (_, closed)) in producers {
        if !closed {
            problems.push(format!("producer {producer} was not closed"));
        }
    }
    for consumer in crate::consumer_action_validation::unclosed(consumers) {
        problems.push(format!("consumer {consumer} was not closed"));
    }
    for (client, shutdown) in clients {
        if !shutdown {
            problems.push(format!("client {client} was not shut down"));
        }
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
