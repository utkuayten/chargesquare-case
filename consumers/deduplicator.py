"""
Event Deduplicator
==================

Filters duplicate events that arise from Kafka's at-least-once delivery
guarantee (producer retries, consumer-group rebalances, offset replay).

Unique key: ``event_id``
  Each event is assigned a UUID at the simulator.  event_id is the most
  granular identifier — even two consecutive METER_UPDATEs for the same
  session carry distinct event_ids — so it is safe as the sole dedup key.
  Alternatives considered:
    - session_id + timestamp: not unique within a session (same session can
      have multiple events at the same second).
    - station_id + connector_id + timestamp: collides if the clock is coarse.

Where to store state: two-tier architecture
-------------------------------------------
Tier 1 — In-memory LRU (primary, zero-latency)
  An OrderedDict bounded at ``maxsize`` entries.  At 10 k eps with a
  1 M-entry cap the cache covers ~100 seconds of unique events — sufficient
  for the common case where Kafka redeliveries arrive within milliseconds
  to a few seconds of the original.

Tier 2 — Redis SET NX (cross-restart persistence)
  On consumer restart the in-memory cache is cold.  Each event_id is also
  written to Redis with a TTL so that duplicates spanning a restart are
  still caught.  ``SET key 1 NX EX ttl`` is atomic, making this safe for
  multiple concurrent consumer instances.

TTL rationale (default 1 h)
  Kafka auto-commit fires every 5 s.  After a crash the consumer replays
  from the last committed offset, so the worst-case duplicate window is the
  commit interval.  1 h gives a comfortable margin without bloating Redis
  memory (at 10 k eps ≈ 36 M keys / hour, ~1.5 GB Redis RAM — acceptable
  for this scale; at 100 k eps consider a Redis Bloom filter instead).

Handling post-TTL duplicates
  If a duplicate slips through after TTL expiry:
    - Redis consumer: INCR/DECR and ZINCRBY operations are idempotent enough
      that the small over-count self-corrects on the next session stop.
    - ClickHouse consumer: charging_events uses ReplacingMergeTree, which
      deduplicates on event_id during background merges — stale re-inserts
      are harmless.
"""

from __future__ import annotations

import logging
from collections import OrderedDict
from typing import Optional

import redis as redis_lib

log = logging.getLogger(__name__)

_DEFAULT_PREFIX = "dedup:seen:"
_DEFAULT_TTL_S  = 3_600   # 1 hour


class Deduplicator:
    """
    Two-tier event deduplicator.

    Parameters
    ----------
    maxsize:
        Maximum number of event_ids to hold in the in-memory LRU cache.
        Older entries are evicted when the cap is reached.
    redis_client:
        Optional Redis client.  When provided, unseen event_ids are written
        to Redis so duplicates are caught across consumer restarts.
        Pass ``None`` to use in-memory dedup only (e.g. for ClickHouse
        consumer where ReplacingMergeTree handles any slip-through).
    ttl:
        TTL (seconds) for Redis keys.
    prefix:
        Redis key prefix, e.g. ``"dedup:redis:"`` or ``"dedup:ch:"``.
        Use different prefixes for different consumer instances so they
        maintain independent seen-sets.
    """

    def __init__(
        self,
        maxsize:      int                        = 1_000_000,
        redis_client: Optional[redis_lib.Redis]  = None,
        ttl:          int                        = _DEFAULT_TTL_S,
        prefix:       str                        = _DEFAULT_PREFIX,
    ) -> None:
        self._cache:   OrderedDict[str, None] = OrderedDict()
        self._maxsize  = maxsize
        self._ttl      = ttl
        self._prefix   = prefix
        self._r        = redis_client
        self._hits     = 0
        self._total    = 0

    # ── public API ────────────────────────────────────────────────────────────

    def is_duplicate(self, event_id: str) -> bool:
        """
        Return ``True`` if *event_id* has been seen before.

        Side-effect: marks the event_id as seen on first call, so all
        subsequent calls for the same id return ``True``.
        """
        self._total += 1

        # Tier 1: in-memory LRU
        if event_id in self._cache:
            self._hits += 1
            self._cache.move_to_end(event_id)   # refresh recency
            return True

        # Tier 2: Redis atomic set-if-not-exists
        if self._r is not None:
            key    = self._prefix + event_id
            is_new = self._r.set(key, 1, ex=self._ttl, nx=True)
            if not is_new:
                # Another consumer (or a previous run) already saw this id
                self._hits += 1
                self._cache[event_id] = None
                self._evict()
                return True

        # First occurrence — record in local cache
        self._cache[event_id] = None
        self._evict()
        return False

    @property
    def hit_rate(self) -> float:
        """Fraction of events that were duplicates (0.0–1.0)."""
        return self._hits / max(self._total, 1)

    @property
    def hits(self) -> int:
        return self._hits

    @property
    def total(self) -> int:
        return self._total

    # ── private ───────────────────────────────────────────────────────────────

    def _evict(self) -> None:
        while len(self._cache) > self._maxsize:
            self._cache.popitem(last=False)
