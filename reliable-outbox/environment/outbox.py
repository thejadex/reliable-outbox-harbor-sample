"""Durable outbox implementation entry point. Python standard library only."""


class Store:
    def __init__(self, path):
        self.path = path

    def register(self, tenant, endpoint, active=True):
        raise NotImplementedError("Implement endpoint registration")

    def submit(self, tenant, event_id, payload, *, now):
        raise NotImplementedError("Implement atomic idempotent fanout")

    def claim(self, worker, *, now, lease_seconds, limit):
        raise NotImplementedError("Implement FIFO claims and lease recovery")

    def ack(self, tenant, event_id, endpoint, token, *, now):
        raise NotImplementedError("Implement fenced acknowledgment")

    def fail(self, tenant, event_id, endpoint, token, *, now, retryable=True):
        raise NotImplementedError("Implement fenced retry and terminal failure")

    def snapshot(self, tenant):
        raise NotImplementedError("Implement a consistent delivery snapshot")
