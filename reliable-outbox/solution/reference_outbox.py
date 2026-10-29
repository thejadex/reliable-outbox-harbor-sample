"""Collaboratively prepared reference implementation; not agent-visible."""
import contextlib
import json
import math
import secrets
import sqlite3


def name(value):
    if not isinstance(value, str) or not value:
        raise ValueError("identifier must be a nonempty string")


def clock(value, positive=False):
    try:
        valid = type(value) in (int, float) and math.isfinite(value)
    except (OverflowError, TypeError):
        valid = False
    if not valid or (value <= 0 if positive else value < 0):
        raise ValueError("invalid logical time")


def canonical(payload):
    def visit(value):
        if type(value) is dict:
            if any(type(k) is not str for k in value):
                raise ValueError("JSON object keys must be strings")
            for child in value.values():
                visit(child)
        elif type(value) is list:
            for child in value:
                visit(child)
        elif value is None or type(value) in (str, bool, int):
            return
        elif type(value) is float and math.isfinite(value):
            return
        else:
            raise ValueError("invalid JSON value")
    if type(payload) is not dict:
        raise ValueError("payload must be an object")
    try:
        visit(payload)
        return json.dumps(payload, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False)
    except (RecursionError, TypeError, OverflowError) as exc:
        raise ValueError("invalid payload") from exc


class Store:
    def __init__(self, path):
        self.path = path
        with self._transaction() as db:
            db.execute("CREATE TABLE IF NOT EXISTS endpoints(tenant TEXT, endpoint TEXT, active INTEGER NOT NULL, PRIMARY KEY(tenant,endpoint))")
            db.execute("CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY, tenant TEXT NOT NULL, event_id TEXT NOT NULL, payload TEXT NOT NULL, UNIQUE(tenant,event_id))")
            db.execute("CREATE TABLE IF NOT EXISTS deliveries(seq INTEGER NOT NULL REFERENCES events(seq), endpoint TEXT NOT NULL, state TEXT NOT NULL, attempts INTEGER NOT NULL, available_at REAL NOT NULL, lease_until REAL, token TEXT, worker TEXT, PRIMARY KEY(seq,endpoint))")
            db.execute("CREATE INDEX IF NOT EXISTS delivery_partition ON deliveries(endpoint,seq,state)")

    @contextlib.contextmanager
    def _transaction(self):
        db = sqlite3.connect(self.path, timeout=60, isolation_level=None)
        try:
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def register(self, tenant, endpoint, active=True):
        name(tenant)
        name(endpoint)
        if type(active) is not bool:
            raise ValueError("active must be boolean")
        with self._transaction() as db:
            db.execute("INSERT INTO endpoints VALUES(?,?,?) ON CONFLICT(tenant,endpoint) DO UPDATE SET active=excluded.active", (tenant, endpoint, active))

    def submit(self, tenant, event_id, payload, *, now):
        name(tenant)
        name(event_id)
        clock(now)
        serialized = canonical(payload)
        with self._transaction() as db:
            old = db.execute("SELECT seq,payload FROM events WHERE tenant=? AND event_id=?", (tenant,event_id)).fetchone()
            if old:
                if old[1] != serialized:
                    raise ValueError("event ID conflict")
                return db.execute("SELECT COUNT(*) FROM deliveries WHERE seq=?", (old[0],)).fetchone()[0]
            seq = db.execute("INSERT INTO events(tenant,event_id,payload) VALUES(?,?,?)", (tenant,event_id,serialized)).lastrowid
            db.execute("INSERT INTO deliveries(seq,endpoint,state,attempts,available_at) SELECT ?,endpoint,'pending',0,? FROM endpoints WHERE tenant=? AND active=1 ORDER BY endpoint", (seq,now,tenant))
            return db.execute("SELECT COUNT(*) FROM deliveries WHERE seq=?", (seq,)).fetchone()[0]

    def claim(self, worker, *, now, lease_seconds, limit):
        name(worker)
        clock(now)
        clock(lease_seconds, positive=True)
        if type(limit) is not int or limit <= 0:
            raise ValueError("limit must be a positive integer")
        expiry = now + lease_seconds
        clock(expiry)
        with self._transaction() as db:
            db.execute("UPDATE deliveries SET state='dead',lease_until=NULL,token=NULL,worker=NULL WHERE state='inflight' AND lease_until<=? AND attempts>=5", (now,))
            db.execute("UPDATE deliveries SET state='pending',available_at=lease_until,lease_until=NULL,token=NULL,worker=NULL WHERE state='inflight' AND lease_until<=?", (now,))
            rows = db.execute("""SELECT d.seq,e.tenant,e.event_id,d.endpoint,e.payload,d.attempts
                FROM deliveries d JOIN events e ON e.seq=d.seq
                WHERE d.state='pending' AND d.available_at<=?
                AND NOT EXISTS(SELECT 1 FROM deliveries p JOIN events pe ON pe.seq=p.seq
                  WHERE pe.tenant=e.tenant AND p.endpoint=d.endpoint AND p.seq<d.seq
                  AND p.state NOT IN ('delivered','dead'))
                ORDER BY d.available_at,d.seq,d.endpoint LIMIT ?""", (now,limit)).fetchall()
            result = []
            for seq,tenant,event_id,endpoint,payload,attempts in rows:
                token = secrets.token_hex(32)
                db.execute("UPDATE deliveries SET state='inflight',attempts=attempts+1,lease_until=?,token=?,worker=? WHERE seq=? AND endpoint=?", (expiry,token,worker,seq,endpoint))
                result.append(dict(tenant=tenant,event_id=event_id,endpoint=endpoint,payload=json.loads(payload),attempt=attempts+1,token=token,lease_until=expiry))
            return result

    def _finish(self, tenant, event_id, endpoint, token, now, failure, retryable):
        for value in (tenant,event_id,endpoint,token):
            name(value)
        clock(now)
        if type(retryable) is not bool:
            raise ValueError("retryable must be boolean")
        with self._transaction() as db:
            row = db.execute("SELECT d.seq,d.state,d.attempts,d.lease_until,d.token FROM deliveries d JOIN events e ON e.seq=d.seq WHERE e.tenant=? AND e.event_id=? AND d.endpoint=?", (tenant,event_id,endpoint)).fetchone()
            if not row or row[1] != 'inflight' or row[4] != token or now >= row[3]:
                raise ValueError("invalid or expired ownership")
            seq,_,attempts,_,_ = row
            state = 'pending' if failure and retryable and attempts < 5 else 'dead' if failure else 'delivered'
            if state == 'pending':
                db.execute("UPDATE deliveries SET state=?,available_at=?,lease_until=NULL,token=NULL,worker=NULL WHERE seq=? AND endpoint=?", (state,now+min(2**attempts,32),seq,endpoint))
            else:
                db.execute("UPDATE deliveries SET state=?,lease_until=NULL,token=NULL,worker=NULL WHERE seq=? AND endpoint=?", (state,seq,endpoint))

    def ack(self, tenant, event_id, endpoint, token, *, now):
        self._finish(tenant,event_id,endpoint,token,now,False,True)

    def fail(self, tenant, event_id, endpoint, token, *, now, retryable=True):
        self._finish(tenant,event_id,endpoint,token,now,True,retryable)

    def snapshot(self, tenant):
        name(tenant)
        with self._transaction() as db:
            rows = db.execute("SELECT e.event_id,d.endpoint,e.payload,d.state,d.attempts,d.available_at,d.lease_until FROM deliveries d JOIN events e ON e.seq=d.seq WHERE e.tenant=? ORDER BY e.seq,d.endpoint", (tenant,)).fetchall()
            return [dict(event_id=r[0],endpoint=r[1],payload=json.loads(r[2]),state=r[3],attempts=r[4],available_at=r[5],lease_until=r[6]) for r in rows]
