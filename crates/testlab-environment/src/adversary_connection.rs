//! One Kafka connection applies armed response faults after complete request reads.

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use testlab_schema::{
    ADVERSARY_PROTOCOL_VERSION, AdversaryEvent, AdversaryOutcome, DisconnectPoint,
    ProtocolAdversaryObservation, ProtocolFault,
};

use crate::adversary_frame::{RequestIdentity, parse_request, response};
use crate::adversary_output::EventWriter;
use crate::adversary_state::{AdversaryState, SelectedFault};

const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct ConnectionContext {
    pub(crate) connection: u64,
    pub(crate) endpoint: String,
    pub(crate) topic: String,
    pub(crate) state: Arc<Mutex<AdversaryState>>,
    pub(crate) output: EventWriter,
    pub(crate) next_observation: Arc<AtomicU64>,
    pub(crate) stopping: Arc<AtomicBool>,
}

pub(crate) fn serve(mut peer: TcpStream, context: &ConnectionContext) -> Result<(), String> {
    peer.set_read_timeout(Some(Duration::from_millis(200)))
        .map_err(|error| format!("set Kafka peer read timeout: {error}"))?;
    let mut request = 0_u64;
    while !context.stopping.load(Ordering::Acquire) {
        let frame = match read_frame(&mut peer)? {
            FrameRead::Idle => continue,
            FrameRead::Closed => return Ok(()),
            FrameRead::Frame(frame) => frame,
        };
        request = request
            .checked_add(1)
            .ok_or_else(|| "connection request identity overflow".to_owned())?;
        let identity = parse_request(&frame)?;
        let keep_open = handle_request(&mut peer, context, identity, frame.len() + 4, request)?;
        if !keep_open {
            return Ok(());
        }
    }
    Ok(())
}

fn handle_request(
    peer: &mut TcpStream,
    context: &ConnectionContext,
    identity: RequestIdentity,
    request_bytes: usize,
    request: u64,
) -> Result<bool, String> {
    let selected = context
        .state
        .lock()
        .map_err(|_| "adversary state lock was poisoned".to_owned())?
        .select(identity.api);
    let baseline = response(
        identity,
        &context.endpoint,
        &context.topic,
        identity.correlation_id,
    )?;
    let (written, keep_open, outcome, control_id, retain) = match selected {
        None => {
            let written = write_counted(peer, &baseline)?;
            (
                written,
                written == baseline.len(),
                AdversaryOutcome::Baseline,
                None,
                true,
            )
        }
        Some(selected) => apply_fault(peer, context, identity, &baseline, selected)?,
    };
    if retain && written == baseline.len() {
        context
            .state
            .lock()
            .map_err(|_| "adversary state lock was poisoned".to_owned())?
            .retain_response(identity.api, baseline);
    }
    let observation = ProtocolAdversaryObservation {
        observation: context.next_observation.fetch_add(1, Ordering::AcqRel),
        connection: context.connection,
        request,
        api: identity.api,
        api_version: identity.version,
        correlation_id: identity.correlation_id,
        request_bytes: u32::try_from(request_bytes)
            .map_err(|_| "request byte count exceeded evidence bound".to_owned())?,
        response_bytes: u32::try_from(written)
            .map_err(|_| "response byte count exceeded evidence bound".to_owned())?,
        control_id,
        outcome,
    };
    context.output.emit(&AdversaryEvent::Observation {
        protocol_version: ADVERSARY_PROTOCOL_VERSION,
        observation,
    })?;
    Ok(keep_open)
}

type Applied = (
    usize,
    bool,
    AdversaryOutcome,
    Option<testlab_schema::EnvironmentOperationId>,
    bool,
);

fn apply_fault(
    peer: &mut TcpStream,
    context: &ConnectionContext,
    identity: RequestIdentity,
    baseline: &[u8],
    selected: SelectedFault,
) -> Result<Applied, String> {
    let fault = selected.fault.clone();
    let (written, keep_open, retain) = match selected.fault {
        ProtocolFault::PartialFrame { bytes } => {
            let limit = usize::try_from(bytes).unwrap_or(usize::MAX);
            let prefix = limit.min(baseline.len().saturating_sub(1));
            (write_counted(peer, &baseline[..prefix])?, false, false)
        }
        ProtocolFault::WrongCorrelationId { delta } => {
            let correlation = identity.correlation_id.wrapping_add(delta);
            let wrong = response(identity, &context.endpoint, &context.topic, correlation)?;
            let written = write_counted(peer, &wrong)?;
            (written, written == wrong.len(), false)
        }
        ProtocolFault::StaleResponse => {
            let stale = context
                .state
                .lock()
                .map_err(|_| "adversary state lock was poisoned".to_owned())?
                .stale_response(identity.api)
                .ok_or_else(|| "no prior complete response from another API exists".to_owned())?;
            let written = write_counted(peer, &stale)?;
            (written, written == stale.len(), false)
        }
        ProtocolFault::Stall { duration_ms } => {
            thread::sleep(Duration::from_millis(duration_ms));
            let written = write_counted(peer, baseline)?;
            (written, written == baseline.len(), true)
        }
        ProtocolFault::Disconnect { point } => match point {
            DisconnectPoint::AfterRequest | DisconnectPoint::BeforeResponse => (0, false, false),
            DisconnectPoint::AfterResponse => {
                let written = write_counted(peer, baseline)?;
                (written, false, true)
            }
        },
    };
    Ok((
        written,
        keep_open,
        AdversaryOutcome::FaultApplied { fault },
        Some(selected.operation_id),
        retain,
    ))
}

fn write_counted(peer: &mut TcpStream, mut bytes: &[u8]) -> Result<usize, String> {
    let total = bytes.len();
    while !bytes.is_empty() {
        match peer.write(bytes) {
            Ok(0) => break,
            Ok(written) => bytes = &bytes[written..],
            Err(error) if is_peer_close(&error) => break,
            Err(error) => return Err(format!("write Kafka response: {error}")),
        }
    }
    Ok(total - bytes.len())
}

enum FrameRead {
    Idle,
    Closed,
    Frame(Vec<u8>),
}

fn read_frame(peer: &mut TcpStream) -> Result<FrameRead, String> {
    let mut available = [0_u8; 1];
    match peer.peek(&mut available) {
        Ok(0) => return Ok(FrameRead::Closed),
        Ok(_) => {}
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            ) =>
        {
            return Ok(FrameRead::Idle);
        }
        Err(error) if is_peer_close(&error) => return Ok(FrameRead::Closed),
        Err(error) => return Err(format!("peek Kafka request: {error}")),
    }
    let mut prefix = [0_u8; 4];
    if let Err(error) = peer.read_exact(&mut prefix) {
        return if is_peer_close(&error) {
            Ok(FrameRead::Closed)
        } else {
            Err(format!("read Kafka frame prefix: {error}"))
        };
    }
    let length = i32::from_be_bytes(prefix);
    let length = usize::try_from(length)
        .map_err(|_| "Kafka request declared a negative frame length".to_owned())?;
    if !(8..=MAX_FRAME_BYTES).contains(&length) {
        return Err(format!(
            "Kafka request frame length {length} is outside bounds"
        ));
    }
    let mut frame = vec![0_u8; length];
    peer.read_exact(&mut frame)
        .map_err(|error| format!("read Kafka frame body: {error}"))?;
    Ok(FrameRead::Frame(frame))
}

fn is_peer_close(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::UnexpectedEof
    )
}
