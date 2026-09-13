//! Legacy IDs and detailed quorum members normalize into one status shape.

use serde::Deserialize;

use super::{StatusReplica, directory_id, invalid};
use crate::observer_error::ObserverError;

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StatusMembership {
    Detailed(Vec<StatusReplica>),
    LegacyIds(Vec<i32>),
}

pub(super) fn parse(value: &str, kind: &str) -> Result<Vec<StatusReplica>, ObserverError> {
    let membership = serde_json::from_str::<StatusMembership>(value)
        .map_err(|error| invalid(format!("invalid {kind} JSON: {error}")))?;
    Ok(match membership {
        StatusMembership::Detailed(replicas) => replicas,
        StatusMembership::LegacyIds(ids) => ids
            .into_iter()
            .map(|id| StatusReplica {
                id,
                directory_id: None,
                endpoints: Vec::new(),
            })
            .collect(),
    })
}

pub(super) fn canonicalize(
    replicas: &mut [StatusReplica],
    kind: &str,
) -> Result<(), ObserverError> {
    for replica in replicas.iter_mut() {
        if replica.id < 0 {
            return Err(invalid(format!("{kind} contained a negative node ID")));
        }
        let directory_id = directory_id(replica.directory_id.as_deref())?;
        replica.directory_id = directory_id;
    }
    replicas.sort_unstable_by_key(|replica| replica.id);
    if replicas.windows(2).any(|pair| pair[0].id == pair[1].id) {
        return Err(invalid(format!("{kind} repeated a node ID")));
    }
    Ok(())
}
