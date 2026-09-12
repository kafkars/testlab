//! Expected-event dispatch is isolated from the wire expectation type.

use crate::run_error::RunFailure;
use crate::runner_protocol::ExpectedEvent;
use crate::runner_protocol_admin::classify_admin;
use crate::runner_protocol_admin_config::classify as classify_admin_config;
use crate::runner_protocol_admin_group_batch::classify as classify_admin_group_batch;
use crate::runner_protocol_event::{EventDisposition, classify_core};
use crate::runner_protocol_family::classify_group;
use testlab_schema::AdapterEvent;

impl ExpectedEvent {
    pub(crate) fn classify(&self, event: &AdapterEvent) -> Result<EventDisposition, RunFailure> {
        if matches!(event, AdapterEvent::CommandFailed { .. }) {
            return Ok(EventDisposition::Complete);
        }
        if let Some(disposition) = crate::runner_protocol_concurrent::classify(self, event) {
            return disposition;
        }
        if let Some(disposition) = classify_group(self, event) {
            return disposition;
        }
        if let Some(disposition) = crate::runner_protocol_share::classify(self, event) {
            return disposition;
        }
        if let Some(disposition) = classify_admin_config(self, event) {
            return disposition;
        }
        if let Some(disposition) = classify_admin_group_batch(self, event) {
            return disposition;
        }
        if let Some(disposition) = crate::runner_protocol_admin_acl::classify(self, event) {
            return disposition;
        }
        if let Some(disposition) = crate::runner_protocol_admin_client_quota::classify(self, event)
        {
            return disposition;
        }
        if let Some(disposition) = crate::runner_protocol_admin_user_scram::classify(self, event) {
            return disposition;
        }
        if let Some(disposition) = crate::runner_protocol_admin_share_group::classify(self, event) {
            return disposition;
        }
        if let Some(disposition) = crate::runner_protocol_admin_producers::classify(self, event) {
            return disposition;
        }
        if let Some(disposition) = crate::runner_protocol_admin_transactions::classify(self, event)
        {
            return disposition;
        }
        if let Some(disposition) = classify_admin(self, event) {
            return disposition;
        }
        if let Some(disposition) = crate::runner_protocol_cancel::classify(self, event) {
            return disposition;
        }
        crate::runner_protocol_transaction::classify(self, event)
            .unwrap_or_else(|| classify_core(self, event))
    }
}
