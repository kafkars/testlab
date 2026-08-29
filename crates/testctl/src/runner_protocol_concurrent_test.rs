//! Concurrent protocol tests reject foreign actors while accepting exact ordered joins.

use std::collections::BTreeSet;

use testlab_schema::{ActorId, AdapterEvent, ConcurrencyId, OperationId, TerminalStatus};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};
use crate::runner_protocol_concurrent::ConcurrentExpectation;

#[test]
fn start_requires_exact_ordered_actor_membership() {
    let expected = ExpectedEvent::ConcurrentActorsStarted(expectation());
    let event = AdapterEvent::ConcurrentActorsStarted {
        concurrency_id: id(ConcurrencyId::new("group-1")),
        actor_ids: vec![
            id(ActorId::new("send-actor")),
            id(ActorId::new("receive-actor")),
        ],
    };

    assert_eq!(
        expected
            .classify(&event)
            .unwrap_or_else(|error| panic!("classify start: {error}")),
        EventDisposition::Complete
    );

    let reversed = AdapterEvent::ConcurrentActorsStarted {
        concurrency_id: id(ConcurrencyId::new("group-1")),
        actor_ids: vec![
            id(ActorId::new("receive-actor")),
            id(ActorId::new("send-actor")),
        ],
    };
    assert!(expected.classify(&reversed).is_err());
}

#[test]
fn join_accepts_only_declared_actor_operations() {
    let expected = ExpectedEvent::ConcurrentActorsCompleted(expectation());
    let send = id(OperationId::new("send-op"));

    assert_eq!(
        expected
            .classify(&AdapterEvent::OperationAccepted {
                operation_id: send.clone(),
            })
            .unwrap_or_else(|error| panic!("classify admission: {error}")),
        EventDisposition::Continue
    );
    assert_eq!(
        expected
            .classify(&AdapterEvent::OperationTerminal {
                operation_id: send,
                status: TerminalStatus::Acknowledged,
                code: None,
                offset: Some(1),
            })
            .unwrap_or_else(|error| panic!("classify terminal: {error}")),
        EventDisposition::Continue
    );
    assert!(
        expected
            .classify(&AdapterEvent::OperationAccepted {
                operation_id: id(OperationId::new("foreign-op")),
            })
            .is_err()
    );
}

#[test]
fn join_completes_only_after_exact_ordered_group_completion() {
    let expected = ExpectedEvent::ConcurrentActorsCompleted(expectation());
    let completed = AdapterEvent::ConcurrentActorsCompleted {
        concurrency_id: id(ConcurrencyId::new("group-1")),
        actor_ids: vec![
            id(ActorId::new("send-actor")),
            id(ActorId::new("receive-actor")),
        ],
    };

    assert_eq!(
        expected
            .classify(&completed)
            .unwrap_or_else(|error| panic!("classify completion: {error}")),
        EventDisposition::Complete
    );
}

fn expectation() -> ConcurrentExpectation {
    let send = id(OperationId::new("send-op"));
    let receive = id(OperationId::new("receive-op"));
    ConcurrentExpectation {
        concurrency_id: id(ConcurrencyId::new("group-1")),
        actors: vec![
            (id(ActorId::new("send-actor")), send.clone()),
            (id(ActorId::new("receive-actor")), receive.clone()),
        ],
        sends: BTreeSet::from([send]),
        receives: BTreeSet::from([receive]),
    }
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture identity: {error}"))
}
