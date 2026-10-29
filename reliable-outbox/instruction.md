# Durable event outbox

Implement `Store` in `/app/outbox.py` using Python 3.12 and its standard library. A dispatch service has lost events and sent duplicates after concurrent workers restarted. Replace its unfinished storage layer with an on-disk SQLite outbox. No HTTP service or external network access is needed: endpoints are opaque identifiers, and delivery is simulated through the API below.

The database path is supplied to `Store(path)`. Multiple instances and processes may use the same path. Each operation must be atomic and committed before returning; reopening a store must preserve all state. A failed operation must not partially change state. Serialize competing writers, without making callers retry normal SQLite lock contention. Concurrent initialization must also work. Use real durable transactions, not process-local state or a lock file that survives a crashed worker.

## Input rules

Every tenant, endpoint, event ID, worker and token argument must be a nonempty string. `active` and `retryable` must be actual booleans. `now` must be a nonnegative finite integer or float, excluding booleans. `lease_seconds` must be a positive finite integer or float, excluding booleans. `limit` must be a positive integer, excluding booleans. Invalid arguments raise `ValueError`. All API validation errors must use `ValueError`; filesystem and database infrastructure errors may propagate.

Payloads must be dictionaries whose keys, at every dictionary depth, are strings, and whose values recursively contain only dictionaries, lists, strings, booleans, null, integers and finite floats. Tuples and custom objects are invalid. Store payloads as JSON with `sort_keys=True`, `separators=(",", ":")`, `ensure_ascii=False`, and `allow_nan=False`. This serialized text defines identity; `1` and `1.0` are different. Input objects and returned objects must not alias persisted data. Callers supply logical times, so do not consult the wall clock or sleep for retry delays. Supplied times and resulting deadlines must fit finite SQLite REAL values. Assume supplied times are nondecreasing across committed time-bearing operations.

## API and behavior

`register(tenant, endpoint, active=True) -> None`

Create or update the tenant's subscription. Repeating it is harmless. An inactive subscription receives no new events. Changing subscriptions never changes deliveries already created. Tenant namespaces are independent.

`submit(tenant, event_id, payload, *, now) -> int`

Create an event and one delivery for every currently active endpoint of that tenant, in one transaction. Save the endpoint set at first submission. Return that event's delivery count. A repeat with the identical canonical payload returns the original count without adding deliveries, including when it originally had zero endpoints. A repeat with a different payload raises `ValueError`. This must also hold under concurrent submissions. Event insertion order is the commit order of new events, across all tenants. Retries do not change that order.

Every new delivery begins with `state="pending"`, `attempts=0`, `available_at=now`, and `lease_until=None`.

`claim(worker, *, now, lease_seconds, limit) -> list[dict]`

Before selecting deliveries, recover all expired leases (`lease_until <= now`) in the same transaction. An expired delivery with fewer than five attempts becomes pending, with `available_at` equal to its expired lease time and `lease_until=None`. An expired delivery with five attempts becomes dead, retaining its `available_at` and clearing its lease. Expiry recovery occurs even when there is no delivery to return.

A partition is `(tenant, endpoint)`. Within each partition, only the earliest event not in a terminal state may be claimed. A retry delay or an unexpired lease blocks later events in that partition. Delivered and dead events do not block it. A claim returns at most one delivery from a partition. Select eligible partition heads whose `available_at <= now`, ordered by `(available_at, event insertion order, endpoint)`, capped by `limit`. A busy partition must not block an independent one.

For each selection, increment attempts, set state to `inflight`, set `lease_until=now+lease_seconds`, and issue a fresh unpredictable token of at least 128 bits. A token must never be reused, including after reopening. Concurrent workers must not receive overlapping live ownership. `worker` identifies a claimant but does not replace the token as proof of ownership.

Return exactly these keys per claim: `tenant`, `event_id`, `endpoint`, `payload`, `attempt`, `token`, `lease_until`. The payload is decoded JSON; `attempt` starts at one.

`ack(tenant, event_id, endpoint, token, *, now) -> None`

Accept only a matching token on an inflight delivery with `now < lease_until`. Mark it delivered and clear its lease. Unknown deliveries, stale tokens, already terminal deliveries and expired tokens raise `ValueError` without changing state. In particular, invalid acknowledgments must not perform lease recovery.

`fail(tenant, event_id, endpoint, token, *, now, retryable=True) -> None`

Apply the same ownership validation as `ack`. If retryable and fewer than five attempts have been claimed, make it pending at `now + min(2 ** attempts, 32)` and clear its lease. Otherwise make it dead, keeping its previous `available_at` and clearing its lease. A terminal failure releases the partition for subsequent claims. Invalid failures do not recover leases or mutate state.

`snapshot(tenant) -> list[dict]`

Read one consistent snapshot, sorted by event insertion order then endpoint. Return exactly `event_id`, `endpoint`, `payload`, `state`, `attempts`, `available_at`, `lease_until` per delivery. Snapshotting does not recover leases. A tenant with no deliveries returns `[]`.

## Completion

Preserve the public signatures and implement the complete contract. Your implementation must handle process restarts, concurrent initialization and claims, duplicate submissions, empty fanouts, retry exhaustion, stale workers, ordering across partitions and malformed inputs. You may add standard-library modules under `/app`. Keep the API synchronous. No external services, credentials, third-party packages or background daemons are required.
