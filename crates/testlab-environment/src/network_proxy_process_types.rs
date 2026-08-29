//! Parent-side proxy process types expose bounded controls and terminal evidence.

use std::io::BufWriter;
use std::path::Path;
use std::process::{Child, ChildStdin};
use std::time::Instant;

use testlab_schema::{EnvironmentOperationId, NetworkProxyObservation, NetworkProxyRoute};

use crate::compose_types::ComposePhase;
use crate::network_proxy_process_io::NetworkProcessReaders;

/// Inputs needed to start one external real-cluster network proxy.
#[derive(Clone, Debug)]
pub struct NetworkProxyProcessRequest<'a> {
    /// Current testctl executable used for the private worker subcommand.
    pub program: &'a Path,
    /// Working directory for the child process.
    pub repository_root: &'a Path,
    /// Exact routes in one-based broker order.
    pub routes: &'a [NetworkProxyRoute],
    /// Stable terminal environment operation identity.
    pub operation_id: EnvironmentOperationId,
    /// Diagnostic process start time.
    pub started_unix_ms: u64,
}

/// Owned external network proxy with synchronous scenario controls.
#[derive(Debug)]
pub struct RunningNetworkProxy {
    pub(crate) child: Option<Child>,
    pub(crate) stdin: Option<BufWriter<ChildStdin>>,
    pub(crate) readers: Option<NetworkProcessReaders>,
    pub(crate) routes: Vec<NetworkProxyRoute>,
    pub(crate) observations: Vec<NetworkProxyObservation>,
    pub(crate) fatal: Option<String>,
    pub(crate) operation_id: EnvironmentOperationId,
    pub(crate) program: String,
    pub(crate) args: Vec<String>,
    pub(crate) started_unix_ms: u64,
    pub(crate) started: Instant,
}

/// Proxy terminal phase plus observations not yet consumed by the runner.
#[derive(Clone, Debug, Default)]
pub struct NetworkProxyFinish {
    /// One external process terminal and bounded artifacts.
    pub phase: ComposePhase,
    /// Independently observed completed network effects.
    pub observations: Vec<NetworkProxyObservation>,
}
