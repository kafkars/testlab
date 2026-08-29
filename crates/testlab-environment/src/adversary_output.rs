//! Serialized adversary stdout preserves one valid JSON object per line.

use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use testlab_schema::AdversaryEvent;

#[derive(Clone, Debug, Default)]
pub(crate) struct EventWriter {
    lock: Arc<Mutex<()>>,
}

impl EventWriter {
    pub(crate) fn emit(&self, event: &AdversaryEvent) -> Result<(), String> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| "adversary stdout lock was poisoned".to_owned())?;
        let stdout = io::stdout();
        let mut writer = stdout.lock();
        serde_json::to_writer(&mut writer, event)
            .map_err(|error| format!("serialize adversary event: {error}"))?;
        writer
            .write_all(b"\n")
            .and_then(|()| writer.flush())
            .map_err(|error| format!("write adversary event: {error}"))
    }
}
