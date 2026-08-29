//! Serialized network-proxy stdout preserves one JSON object per line.

use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use testlab_schema::NetworkProxyEvent;

#[derive(Clone, Debug, Default)]
pub(crate) struct NetworkEventWriter {
    lock: Arc<Mutex<()>>,
}

impl NetworkEventWriter {
    pub(crate) fn emit(&self, event: &NetworkProxyEvent) -> Result<(), String> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| "network proxy stdout lock was poisoned".to_owned())?;
        let stdout = io::stdout();
        let mut writer = stdout.lock();
        serde_json::to_writer(&mut writer, event)
            .map_err(|error| format!("serialize network proxy event: {error}"))?;
        writer
            .write_all(b"\n")
            .and_then(|()| writer.flush())
            .map_err(|error| format!("write network proxy event: {error}"))
    }
}
