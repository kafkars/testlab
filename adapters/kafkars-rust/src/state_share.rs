//! Candidate-only share state leaves the published adapter capability-exact.

use testlab_schema::{ClientId, ConsumerId};

use crate::state::AdapterState;

#[cfg(kafkars_share_candidate)]
impl AdapterState {
    pub(super) fn share_contains(&self, consumer_id: &ConsumerId) -> bool {
        self.share_consumers.contains(consumer_id)
    }

    pub(super) fn share_has_owner(&self, client_id: &ClientId) -> bool {
        self.share_consumers.has_owner(client_id)
    }

    pub(super) fn share_is_empty(&self) -> bool {
        self.share_consumers.is_empty()
    }
}

#[cfg(not(kafkars_share_candidate))]
impl AdapterState {
    #[expect(
        clippy::unused_self,
        reason = "the stable adapter has no candidate share state"
    )]
    pub(super) fn share_contains(&self, _consumer_id: &ConsumerId) -> bool {
        false
    }

    #[expect(
        clippy::unused_self,
        reason = "the stable adapter has no candidate share state"
    )]
    pub(super) fn share_has_owner(&self, _client_id: &ClientId) -> bool {
        false
    }

    #[expect(
        clippy::unused_self,
        reason = "the stable adapter has no candidate share state"
    )]
    pub(super) fn share_is_empty(&self) -> bool {
        true
    }
}
