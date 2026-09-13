//! Protocol-focused schema tests share one top-level registration slot.

#[cfg(test)]
#[path = "protocol_adversary_test.rs"]
mod adversary;
#[cfg(test)]
#[path = "assigned_fetch_evidence_test.rs"]
mod assigned_fetch_evidence;
#[cfg(test)]
#[path = "protocol_group_test.rs"]
mod group;
#[cfg(test)]
#[path = "network_proxy_test.rs"]
mod network_proxy;
#[cfg(test)]
#[path = "share_acknowledgement_protocol_test.rs"]
mod share_acknowledgement;
