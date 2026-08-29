//! Concurrent adapter tests reject invalid groups before public handles are touched.

use testlab_schema::{AdapterCommand, CommandId, ConcurrencyId, StartConcurrentActorsCommand};

use crate::protocol_concurrent::dispatch;
use crate::state::AdapterState;

#[test]
fn concurrent_group_requires_at_least_two_actors() {
    let mut state = AdapterState::default();
    let mut output = Vec::new();
    let result = dispatch(
        &mut state,
        &mut output,
        id(CommandId::new("start")),
        AdapterCommand::StartConcurrentActors(StartConcurrentActorsCommand {
            concurrency_id: id(ConcurrencyId::new("group-1")),
            actors: Vec::new(),
        }),
    );

    assert!(result.is_err());
    assert!(output.is_empty());
    assert!(state.concurrent_group.is_none());
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture identity: {error}"))
}
