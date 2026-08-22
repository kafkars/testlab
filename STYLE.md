# Style

- Prefer explicit histories and verdicts to hidden callbacks or implicit state.
- Use one absolute scenario deadline.
- A client-reported acknowledgment is evidence, not broker-visible truth.
- Every retained event, process, file, and background thread has one owner.
- Normalize semantics across languages without normalizing away uncertainty.
- Keep control messages small, versioned, and forward-compatible.
- Keep manifests readable without opening Rust code.
- Use Kafka vocabulary consistently.
