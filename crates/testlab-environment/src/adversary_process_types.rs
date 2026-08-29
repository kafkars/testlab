//! Adversary process ownership types expose only bounded environment controls.

use std::io::BufWriter;
use std::path::Path;
use std::process::{Child, ChildStdin};
use std::time::Instant;

use testlab_schema::{EnvironmentOperationId, ProtocolAdversaryObservation};

use crate::adversary_process_io::ProcessReaders;

/// Inputs needed to start one external adversary process.
#[derive(Clone, Debug)]
pub struct AdversaryProcessRequest<'a> {
    /// Current testctl executable used for the private worker subcommand.
    pub program: &'a Path,
    /// Working directory for the child process.
    pub repository_root: &'a Path,
    /// Sole baseline topic exposed by the adversary.
    pub topic: &'a str,
    /// Stable terminal environment operation identity.
    pub operation_id: EnvironmentOperationId,
    /// Diagnostic process start time.
    pub started_unix_ms: u64,
}

/// Owned external adversary process with synchronous scenario controls.
#[derive(Debug)]
pub struct RunningAdversary {
    pub(crate) child: Option<Child>,
    pub(crate) stdin: Option<BufWriter<ChildStdin>>,
    pub(crate) readers: Option<ProcessReaders>,
    pub(crate) endpoint: String,
    pub(crate) observations: Vec<ProtocolAdversaryObservation>,
    pub(crate) fatal: Option<String>,
    pub(crate) operation_id: EnvironmentOperationId,
    pub(crate) program: String,
    pub(crate) args: Vec<String>,
    pub(crate) started_unix_ms: u64,
    pub(crate) started: Instant,
}
