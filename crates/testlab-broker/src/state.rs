//! Broker state owns one-shot behaviors, offsets, observations, and health.

use std::collections::VecDeque;

use testlab_schema::{BrokerBehavior, BrokerObservation, ByteString};

use crate::{ModelBrokerRequest, ModelBrokerResponse, ModelBrokerResponseStatus};

#[derive(Debug, Default)]
pub(crate) struct BrokerState {
    next_behaviors: VecDeque<BrokerBehavior>,
    observations: Vec<BrokerObservation>,
    next_offset: i64,
    failure: Option<String>,
}

impl BrokerState {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn push_behavior(&mut self, behavior: BrokerBehavior) {
        self.next_behaviors.push_back(behavior);
    }

    pub(crate) fn apply(&mut self, request: ModelBrokerRequest) -> Result<BrokerAction, String> {
        let behavior = self
            .next_behaviors
            .pop_front()
            .unwrap_or(BrokerBehavior::Acknowledge);
        match behavior {
            BrokerBehavior::Acknowledge => {
                let offset = self.observe(request)?;
                Ok(BrokerAction::Respond(acknowledged(offset)))
            }
            BrokerBehavior::AcceptAndDropResponse => {
                self.observe(request)?;
                Ok(BrokerAction::DropResponse)
            }
            BrokerBehavior::Reject => Ok(BrokerAction::Respond(ModelBrokerResponse {
                status: ModelBrokerResponseStatus::Rejected,
                offset: None,
                code: Some("model_rejected".to_owned()),
            })),
            BrokerBehavior::DuplicateAndAcknowledge => {
                let duplicate = request.clone();
                let offset = self.observe(request)?;
                self.observe(duplicate)?;
                Ok(BrokerAction::Respond(acknowledged(offset)))
            }
            BrokerBehavior::CorruptAndAcknowledge => {
                let mut corrupted = request;
                corrupted.record.value = Some(ByteString::utf8("corrupted-by-model-broker"));
                let offset = self.observe(corrupted)?;
                Ok(BrokerAction::Respond(acknowledged(offset)))
            }
        }
    }

    pub(crate) fn observations(&self) -> Vec<BrokerObservation> {
        self.observations.clone()
    }

    pub(crate) fn set_failure(&mut self, diagnostic: String) {
        if self.failure.is_none() {
            self.failure = Some(diagnostic);
        }
    }

    pub(crate) fn failure(&self) -> Option<String> {
        self.failure.clone()
    }

    fn observe(&mut self, request: ModelBrokerRequest) -> Result<i64, String> {
        let digest = request
            .record
            .digest()
            .map_err(|error| format!("invalid record: {error}"))?;
        let offset = self.next_offset;
        self.next_offset = self
            .next_offset
            .checked_add(1)
            .ok_or_else(|| "model broker offset overflow".to_owned())?;
        let observation = u64::try_from(self.observations.len())
            .map_err(|_| "observation ordinal overflow".to_owned())?;
        self.observations.push(BrokerObservation {
            observation,
            offset,
            operation_id: request.operation_id,
            record: request.record,
            digest,
        });
        Ok(offset)
    }
}

fn acknowledged(offset: i64) -> ModelBrokerResponse {
    ModelBrokerResponse {
        status: ModelBrokerResponseStatus::Acknowledged,
        offset: Some(offset),
        code: None,
    }
}

#[derive(Debug)]
pub(crate) enum BrokerAction {
    Respond(ModelBrokerResponse),
    DropResponse,
}
