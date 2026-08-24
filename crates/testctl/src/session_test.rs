//! Session tests preserve failed-scenario cleanup as an explicit abort.

use testlab_schema::AdapterCommand;

use super::runner_protocol::ExpectedEvent;
use super::session::scenario_failure_settlement;

#[test]
fn scenario_failure_aborts_instead_of_claiming_clean_finish() {
    let (command, expected) = scenario_failure_settlement();

    assert_eq!(command, AdapterCommand::Abort);
    assert!(matches!(expected, ExpectedEvent::Aborted));
}
