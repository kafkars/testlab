//! Leader-election targets preserve exact scenario-to-wire correlation.

use testlab_schema::{AdapterCommand, ElectLeadersCommand, OperationId, ScenarioAction};

use crate::observer_admin_target::{AdminTarget, TargetMatch};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LeaderElectionTarget {
    pub(super) operation_id: OperationId,
    pub(super) election_type: testlab_schema::AdminLeaderElectionType,
    pub(super) partitions: Vec<testlab_schema::LeaderElectionSelection>,
}

pub(super) fn match_action(action: &ScenarioAction) -> Option<TargetMatch> {
    let ScenarioAction::ElectLeaders(action) = action else {
        return None;
    };
    let partitions = action
        .targets
        .clone()
        .unwrap_or_else(|| action.required_partitions.clone());
    Some((
        AdapterCommand::ElectLeaders(ElectLeadersCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            election_type: action.election_type,
            targets: action.targets.clone(),
            timeout_ms: action.timeout_ms,
        }),
        AdminTarget::LeaderElection(LeaderElectionTarget {
            operation_id: action.operation_id.clone(),
            election_type: action.election_type,
            partitions,
        }),
    ))
}
