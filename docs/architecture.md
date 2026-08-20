# Architecture

Ingestion appends a destination or event record to an fsync'd journal before updating memory. Startup replays the journal to recover exact logical state. Workers pull due events with bounded concurrency, sign a CloudEvents envelope, and record each state transition.

The reference journal intentionally optimizes for inspectability. A production PostgreSQL adapter would claim rows with `FOR UPDATE SKIP LOCKED`, lease expirations, and per-destination advisory locks while preserving the same state machine.
