"""
Event-Time Watermark & Late-Event Detection
============================================

2b Design decisions
-------------------

Event-time vs processing-time
  We use **event-time** (the ``timestamp`` field embedded in the event)
  for all window computations — not the time the message arrived at the
  consumer.

  Why event-time?
    A METER_UPDATE event records the energy delivered *at that moment on
    the charger*.  If the event is delayed 3 minutes by a network retry
    and we use processing-time, the energy would be counted in the wrong
    1-minute window, distorting per-minute consumption charts and SLA
    reports.  Event-time ensures every watt-hour lands in the window that
    matches the physical reality on the ground.

  Why not processing-time?
    Processing-time is only safe when events arrive in strict order with
    negligible delay — an assumption that explicitly does NOT hold here
    (the spec mentions network delays and retry mechanisms).

Watermark mechanics
  The watermark W(t) = max(observed event timestamps) − max_lateness.
  Events with event_time < W(t) are considered "late" and are not used to
  update windows that have already been published.

  ``max_lateness`` defaults to 5 minutes (``MAX_LATENESS_S`` env var).
  This means:
    - Windows stay open for 5 minutes past the latest seen event.
    - Events arriving within that window are accepted normally.
    - Events arriving after that window are routed to the dead-letter file.

  Choice of 5 minutes is a trade-off:
    - Too short → legitimate late events (e.g. a charger that went offline
      briefly and buffered events) are silently discarded.
    - Too long → window state must be kept in memory longer and
      already-published aggregates need to be updated more frequently.
    - 5 min covers the P99 retry/delay window for the OCPP retry policy
      (3 retries × 90 s back-off ≈ 4.5 min).

Handling late events that pass the watermark
  Late events are written to the dead-letter file with reason
  ``late_event:<seconds_late>`` so they can be:
    1. Inspected offline to tune the watermark.
    2. Replayed in a separate batch job that recomputes affected windows.

Would we update already-published window results?
  YES — for the ClickHouse analytics store.  The table engine is
  ReplacingMergeTree (see ``charging_events``), which is designed for
  exactly this: you can re-insert a corrected row and ClickHouse will
  deduplicate on event_id during background merges.  This gives us
  "eventually consistent" window corrections at minimal cost — no need
  for a transactional update API.

  For the Redis real-time store we do NOT update published counters for
  late events because:
    a) Redis counters are approximate operational metrics, not billing data.
    b) The active-session counters use a floor-at-zero Lua script; a
       retroactive DECR for a session that has long since been cleared
       would corrupt the counter.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

MAX_LATENESS_S: int = int(os.getenv("WATERMARK_MAX_LATENESS_S", "300"))  # 5 min


class Watermark:
    """
    Tracks the event-time watermark and classifies events as on-time or late.

    Usage::

        wm = Watermark(max_lateness_s=300)
        wm.advance(event_ts)
        if wm.is_late(event_ts):
            dead_letter.write(ev, f"late_event:{wm.seconds_late(event_ts):.0f}")
    """

    def __init__(self, max_lateness_s: int = MAX_LATENESS_S) -> None:
        self._max_ts         = 0.0   # max event timestamp seen (Unix epoch, float)
        self._max_lateness   = max_lateness_s
        self._late_count     = 0
        self._total_count    = 0

    # ── public API ────────────────────────────────────────────────────────────

    def advance(self, event_ts: datetime) -> None:
        """Update the high-water mark with *event_ts* (monotonically advancing)."""
        t = event_ts.timestamp()
        if t > self._max_ts:
            self._max_ts = t

    def is_late(self, event_ts: datetime) -> bool:
        """
        Return ``True`` if *event_ts* is behind the current watermark.

        An event is late when its timestamp is older than
        ``max(seen_timestamps) - max_lateness``.  Returns ``False`` if
        no events have been observed yet (cold start).
        """
        self._total_count += 1
        if self._max_ts == 0.0:
            return False
        late = event_ts.timestamp() < (self._max_ts - self._max_lateness)
        if late:
            self._late_count += 1
        return late

    def seconds_late(self, event_ts: datetime) -> float:
        """How many seconds behind the watermark this event is."""
        return max(0.0, (self._max_ts - self._max_lateness) - event_ts.timestamp())

    @property
    def watermark_utc(self) -> Optional[datetime]:
        """Current watermark as a UTC datetime, or None if not yet set."""
        if self._max_ts == 0.0:
            return None
        return datetime.fromtimestamp(self._max_ts - self._max_lateness, tz=timezone.utc)

    @property
    def late_count(self) -> int:
        return self._late_count

    @property
    def late_rate(self) -> float:
        return self._late_count / max(self._total_count, 1)
