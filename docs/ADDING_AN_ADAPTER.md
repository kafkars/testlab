# Adding an adapter

An adapter translates one packaged client surface to protocol v11. It is not a
runner and not a verifier.

## Checklist

- Implement `hello` and return an exact capability set.
- Keep stdout protocol-only and diagnostics on stderr.
- Preserve command and operation IDs exactly.
- Report admission separately from terminal completion.
- Preserve delivery certainty rather than collapsing failures.
- Use stable normalized codes and bounded diagnostics.
- Emit `command_failed` and exit successfully for a normal public API failure.
- Settle close and shutdown explicitly.
- Exit nonzero after an unrecoverable `fatal` event.
- Use no private test-only client APIs.
- Test an installed or packaged artifact.

## Rust

Call the curated public kafkars API from a resolved subject checkout. Do not
reach into engine internals.

## C

Use a real C or C++ fixture linked against the packaged ABI. Exercise copy-in,
polling, retained events, release order, and shutdown as foreign callers do.

## Java

Load the packaged Java and native artifacts from a clean fixture project. Class
loading, native resolution, dispatch, and shutdown hooks are product behavior.
