//! Exact command verification keeps public request-shape contracts together.

use testlab_schema::{Scenario, Violation};

use crate::index::HistoryIndex;

#[path = "assigned_consumer_assignment_command.rs"]
mod assigned_consumer_assignment_command;
#[path = "assigned_consumer_configuration_command.rs"]
mod assigned_consumer_configuration_command;
#[path = "assigned_consumer_receive_command.rs"]
mod assigned_consumer_receive_command;
#[path = "child_handle_registration.rs"]
mod child_handle_registration;
#[path = "client_creation_command.rs"]
mod client_creation_command;
#[path = "group_consumer_registration.rs"]
mod group_consumer_registration;
#[path = "group_receive_command.rs"]
mod group_receive_command;
#[path = "group_receive_set_command.rs"]
mod group_receive_set_command;
#[path = "lifecycle_request_command.rs"]
mod lifecycle_request_command;
#[path = "producer_configuration_method.rs"]
mod producer_configuration_method;
#[path = "producer_operation_command.rs"]
mod producer_operation_command;
#[path = "share_consumer_registration.rs"]
mod share_consumer_registration;
#[path = "share_lifecycle_command.rs"]
mod share_lifecycle_command;
#[path = "share_receive_command.rs"]
mod share_receive_command;
#[path = "transactional_producer_registration.rs"]
mod transactional_producer_registration;

pub(super) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    producer_configuration_method::verify(scenario, index, violations);
    producer_operation_command::verify(scenario, index, violations);
    assigned_consumer_assignment_command::verify(scenario, index, violations);
    assigned_consumer_configuration_command::verify(scenario, index, violations);
    assigned_consumer_receive_command::verify(scenario, index, violations);
    child_handle_registration::verify(scenario, index, violations);
    client_creation_command::verify(scenario, index, violations);
    group_consumer_registration::verify(scenario, index, violations);
    group_receive_command::verify(scenario, index, violations);
    group_receive_set_command::verify(scenario, index, violations);
    lifecycle_request_command::verify(scenario, index, violations);
    share_consumer_registration::verify(scenario, index, violations);
    share_lifecycle_command::verify(scenario, index, violations);
    share_receive_command::verify(scenario, index, violations);
    transactional_producer_registration::verify(scenario, index, violations);
}
