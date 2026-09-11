//! Batched offset-listing completions retain ordered public outcomes.

use std::collections::BTreeMap;

use testlab_schema::{AdapterEvent, AdminOffsetListingOutcome, OperationId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedAdminOffsetsListing {
    pub(crate) history_sequence: u64,
    pub(crate) outcomes: Vec<AdminOffsetListingOutcome>,
}

#[derive(Debug, Default)]
pub(crate) struct AdminOffsetBatchIndex {
    pub(crate) offsets_listed: BTreeMap<OperationId, Vec<IndexedAdminOffsetsListing>>,
}

impl AdminOffsetBatchIndex {
    pub(super) fn record_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        let AdapterEvent::OffsetsListed(value) = event else {
            return false;
        };
        self.offsets_listed
            .entry(value.operation_id.clone())
            .or_default()
            .push(IndexedAdminOffsetsListing {
                history_sequence: sequence,
                outcomes: value.outcomes.clone(),
            });
        true
    }
}
