"""Subscriber usage tracking: buffered event recording.

Writes are buffered in memory and flushed in batches by a background task. They
are never written inline on a request, and that is the whole design constraint
rather than a micro-optimisation: the documented failure mode of this app under
load is pool exhaustion (see private/gotchas.md on PoolTimeout), where requests
pile up holding connections. Adding a synchronous INSERT to every page view
would put a connection checkout on the hot path for every viewer and make that
failure arrive sooner, in exchange for analytics nobody reads in real time.

The trade accepted in return: a crash or a deploy loses at most one flush
interval of events. For "how many people opened the Congress tab this week" that
is not worth a durability mechanism.

Each uvicorn worker keeps its own buffer and flushes it independently, so with
--workers 4 there are four small periodic writers rather than one contended one.
"""
from __future__ import annotations

import json
import logging
import threading
from collections import deque

logger = logging.getLogger("usage")

# Flush when either trigger fires first. 100 keeps a batch to a single small
# INSERT; 30s bounds how much a restart can lose.
FLUSH_EVERY_N = 100
FLUSH_INTERVAL_SECONDS = 30

# Hard ceiling so a database outage cannot turn this into a memory leak. At that
# point the app is degraded anyway and analytics are the least of it, so the
# oldest events are dropped rather than the newest: a full buffer means the
# recent ones are the ones still worth having.
MAX_BUFFERED = 5000

# Staff traffic would otherwise dominate every count during the working day.
STAFF_EMAIL = "staff"

# Subscriber behavioural data expires rather than accumulating forever. Pruned by
# the nightly ingest, so there is no systemd unit to forget about.
RETENTION_DAYS = 90

_buffer: deque = deque(maxlen=MAX_BUFFERED)
_lock = threading.Lock()


def record(email: str, kind: str, path: str, meta: dict | None = None) -> None:
    """Queue one event. Never raises, never blocks on I/O."""
    with _lock:
        _buffer.append((email, kind, path, json.dumps(meta) if meta else None))


def pending() -> int:
    with _lock:
        return len(_buffer)


def _drain() -> list[tuple]:
    """Remove and return everything currently buffered."""
    with _lock:
        items = list(_buffer)
        _buffer.clear()
    return items


def flush(conn) -> int:
    """Write buffered events. Returns the number written.

    On failure the batch is put BACK at the front of the buffer, so a transient
    database blip delays events rather than dropping them. It is re-appended
    under the same deque maxlen, so a sustained outage still cannot grow memory
    without bound.
    """
    items = _drain()
    if not items:
        return 0
    try:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO usage_event (email, kind, path, meta)"
                " VALUES (%s, %s, %s, %s::jsonb)",
                items,
            )
        return len(items)
    except Exception as exc:
        logger.warning("usage flush failed, re-queueing %d events: %s", len(items), exc)
        with _lock:
            _buffer.extendleft(reversed(items))
        return 0


def prune(conn, days: int = 90) -> int:
    """Delete events older than `days`. Returns rows removed.

    This is subscriber behavioural data, so it expires on a schedule rather than
    accumulating forever. Runs from the nightly ingest so it needs no systemd
    unit of its own.
    """
    cur = conn.execute(
        "DELETE FROM usage_event WHERE ts < now() - make_interval(days => %s)",
        [days],
    )
    return cur.rowcount
