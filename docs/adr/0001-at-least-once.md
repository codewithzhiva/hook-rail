# ADR 0001: At-least-once delivery

Status: accepted. An HTTP sender cannot distinguish a lost response from a lost request. HookRail therefore retries ambiguous failures and exposes stable event IDs so receivers can implement idempotency.
