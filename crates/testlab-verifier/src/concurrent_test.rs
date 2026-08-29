//! Concurrent verifier tests pin exact schedules, actor outcomes, and independent truth.

use testlab_schema::{ActorId, AdapterEvent, CommandId, HistoryPayload, VerdictStatus};

use crate::concurrent_fixture_test::fixture;

#[test]
fn exact_concurrent_producer_consumer_history_passes() {
    let fixture = fixture();

    let verdict = crate::verify(
        &fixture.scenario,
        &fixture.adapter,
        &fixture.history,
        &fixture.observations,
    );

    assert_eq!(verdict.status, VerdictStatus::Passed, "{verdict:?}");
}

#[test]
fn foreign_actor_completion_fails_exact_membership() {
    let mut fixture = fixture();
    let completion = fixture
        .history
        .iter_mut()
        .find_map(|entry| match &mut entry.payload {
            HistoryPayload::AdapterEvent { event } => match &mut event.event {
                AdapterEvent::ConcurrentActorCompleted { actor_id, .. } => Some(actor_id),
                _ => None,
            },
            _ => None,
        });
    let Some(completion) = completion else {
        panic!("actor completion missing");
    };
    *completion =
        ActorId::new("foreign-actor").unwrap_or_else(|error| panic!("foreign actor: {error}"));

    let verdict = crate::verify(
        &fixture.scenario,
        &fixture.adapter,
        &fixture.history,
        &fixture.observations,
    );

    assert!(has_contract(&verdict, "CONCUR-002"));
}

#[test]
fn public_outcome_under_the_wrong_command_fails_window_correlation() {
    let mut fixture = fixture();
    let event = fixture
        .history
        .iter_mut()
        .find_map(|entry| match &mut entry.payload {
            HistoryPayload::AdapterEvent { event }
                if matches!(event.event, AdapterEvent::OperationAccepted { .. }) =>
            {
                Some(event)
            }
            _ => None,
        });
    let Some(event) = event else {
        panic!("operation admission missing");
    };
    event.command_id =
        CommandId::new("start").unwrap_or_else(|error| panic!("command identity: {error}"));

    let verdict = crate::verify(
        &fixture.scenario,
        &fixture.adapter,
        &fixture.history,
        &fixture.observations,
    );

    assert!(has_contract(&verdict, "CONCUR-003"));
}

#[test]
fn missing_broker_record_fails_concurrent_truth() {
    let mut fixture = fixture();
    fixture.observations.clear();

    let verdict = crate::verify(
        &fixture.scenario,
        &fixture.adapter,
        &fixture.history,
        &fixture.observations,
    );

    assert!(has_contract(&verdict, "CONCUR-004"));
}

#[test]
fn wrong_concurrent_receive_record_fails_concurrent_truth() {
    let mut fixture = fixture();
    for entry in &mut fixture.history {
        let HistoryPayload::AdapterEvent { event } = &mut entry.payload else {
            continue;
        };
        if let AdapterEvent::ReceiveCompleted { records, .. } = &mut event.event {
            records.clear();
        }
    }

    let verdict = crate::verify(
        &fixture.scenario,
        &fixture.adapter,
        &fixture.history,
        &fixture.observations,
    );

    assert!(has_contract(&verdict, "CONCUR-004"));
}

fn has_contract(verdict: &testlab_schema::Verdict, contract: &str) -> bool {
    verdict
        .violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
