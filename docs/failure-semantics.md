# Failure semantics

- Failure before journal fsync returns an error and claims no acceptance.
- Failure after fsync is recovered on restart.
- Timeouts, network errors, `429`, and non-`2xx` responses retry.
- Maximum attempts move an event to dead letter.
- Replays reset attempt state but retain the original event ID and payload.
- At-least-once delivery means a receiver may observe a duplicate after an ambiguous network failure.
