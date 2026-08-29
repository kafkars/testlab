//! Shared adversary state selects ordered faults and retains replay material.

use std::collections::VecDeque;

use testlab_schema::{EnvironmentOperationId, KafkaApi, ProtocolFault, ProtocolFaultAction};

#[derive(Clone, Debug)]
pub(crate) struct SelectedFault {
    pub(crate) operation_id: EnvironmentOperationId,
    pub(crate) fault: ProtocolFault,
}

#[derive(Debug, Default)]
pub(crate) struct AdversaryState {
    armed: VecDeque<ProtocolFaultAction>,
    prior_responses: VecDeque<(KafkaApi, Vec<u8>)>,
    fatal: Option<String>,
}

impl AdversaryState {
    pub(crate) fn arm(&mut self, control: ProtocolFaultAction) -> Result<(), String> {
        control.validate()?;
        if self
            .armed
            .iter()
            .any(|candidate| candidate.operation_id == control.operation_id)
        {
            return Err(format!(
                "duplicate adversary control {}",
                control.operation_id
            ));
        }
        self.armed.push_back(control);
        Ok(())
    }

    pub(crate) fn select(&mut self, api: KafkaApi) -> Option<SelectedFault> {
        let position = self.armed.iter().position(|control| control.api == api)?;
        let control = self.armed.get_mut(position)?;
        let selected = SelectedFault {
            operation_id: control.operation_id.clone(),
            fault: control.fault.clone(),
        };
        control.applications -= 1;
        if control.applications == 0 {
            let _removed = self.armed.remove(position);
        }
        Some(selected)
    }

    pub(crate) fn stale_response(&self, api: KafkaApi) -> Option<Vec<u8>> {
        self.prior_responses
            .iter()
            .rev()
            .find(|(candidate, _)| *candidate != api)
            .map(|(_, response)| response.clone())
    }

    pub(crate) fn retain_response(&mut self, api: KafkaApi, response: Vec<u8>) {
        const RETAINED_RESPONSES: usize = 8;
        self.prior_responses.push_back((api, response));
        if self.prior_responses.len() > RETAINED_RESPONSES {
            let _discarded = self.prior_responses.pop_front();
        }
    }

    pub(crate) fn fail(&mut self, diagnostic: String) {
        if self.fatal.is_none() {
            self.fatal = Some(diagnostic);
        }
    }

    pub(crate) fn fatal(&self) -> Option<&str> {
        self.fatal.as_deref()
    }

    pub(crate) fn unconsumed_controls(&self) -> Vec<EnvironmentOperationId> {
        self.armed
            .iter()
            .map(|control| control.operation_id.clone())
            .collect()
    }
}
