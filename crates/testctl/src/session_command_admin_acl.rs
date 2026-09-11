//! ACL scenario actions translate without leaking verifier-owned state onto the wire.

use testlab_schema::{AdapterCommand, ScenarioAction};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    Some(match action {
        ScenarioAction::CreateAcls(value) => (
            AdapterCommand::CreateAcls(value.clone()),
            ExpectedEvent::AclsCreated(value.operation_id.clone()),
        ),
        ScenarioAction::DescribeAcls(value) => (
            AdapterCommand::DescribeAcls(value.clone()),
            ExpectedEvent::AclsDescribed(value.operation_id.clone()),
        ),
        ScenarioAction::DeleteAcls(value) => (
            AdapterCommand::DeleteAcls(value.clone()),
            ExpectedEvent::AclsDeleted(value.operation_id.clone()),
        ),
        _ => return None,
    })
}
