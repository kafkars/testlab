//! One recorder establishes total history order across every trust boundary.

use testlab_schema::{
    AdapterEventEnvelope, BrokerBehavior, BrokerObservation, CommandEnvelope, EnvironmentOperation,
    HarnessError, HistoryEntry, HistoryPayload,
};

use crate::run_error::RunFailure;
use crate::time::unix_ms;

#[derive(Debug, Default)]
pub(crate) struct HistoryRecorder {
    entries: Vec<HistoryEntry>,
    next_sequence: u64,
}

impl HistoryRecorder {
    pub(crate) fn command(&mut self, command: CommandEnvelope) -> Result<(), RunFailure> {
        self.push(HistoryPayload::HarnessCommand { command })
    }

    pub(crate) fn event(&mut self, event: AdapterEventEnvelope) -> Result<(), RunFailure> {
        self.push(HistoryPayload::AdapterEvent { event })
    }

    pub(crate) fn broker_control(&mut self, behavior: BrokerBehavior) -> Result<(), RunFailure> {
        self.push(HistoryPayload::BrokerControl { behavior })
    }

    pub(crate) fn observation(&mut self, observation: BrokerObservation) -> Result<(), RunFailure> {
        self.push(HistoryPayload::BrokerObservation { observation })
    }

    pub(crate) fn environment_operation(
        &mut self,
        operation: EnvironmentOperation,
    ) -> Result<(), RunFailure> {
        self.push(HistoryPayload::EnvironmentOperation { operation })
    }

    pub(crate) fn failure(&mut self, error: HarnessError) -> Result<(), RunFailure> {
        self.push(HistoryPayload::HarnessError { error })
    }

    pub(crate) fn entries(&self) -> &[HistoryEntry] {
        &self.entries
    }

    pub(crate) fn into_entries(self) -> Vec<HistoryEntry> {
        self.entries
    }

    fn push(&mut self, payload: HistoryPayload) -> Result<(), RunFailure> {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.checked_add(1).ok_or_else(|| {
            RunFailure::harness("history_overflow", "history sequence exceeded u64")
        })?;
        self.entries.push(HistoryEntry {
            sequence,
            observed_unix_ms: unix_ms()?,
            payload,
        });
        Ok(())
    }
}
