"""
Schema Validation & Dead-Letter Routing
========================================

Every incoming Kafka message passes through ``validate()`` before it
reaches either the Redis or ClickHouse consumer.  Invalid events are
written to a JSONL dead-letter file for offline inspection and replay.

Validation rules
----------------
- Required base fields present and non-empty:
    event_id, event_type, timestamp, station_id
- event_type is one of the 6 known types
- timestamp is ISO-8601 parseable
- session_id required for: session_start, meter_update, session_stop
- soc_percent: integer in [0, 100]
- power_kw, energy_kwh: non-negative float

Dead-letter format (one JSON object per line)
---------------------------------------------
{
  "rejected_at": "<ISO-8601 UTC>",
  "reason":      "<machine-readable code, e.g. missing_field:event_id>",
  "event":       { ... original event dict ... }
}

The file path defaults to ``dead_letter.jsonl`` in the working directory
and can be overridden with the DEAD_LETTER_PATH env var.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

DEAD_LETTER_PATH = os.getenv("DEAD_LETTER_PATH", "dead_letter.jsonl")

_VALID_EVENT_TYPES = frozenset({
    "session_start", "meter_update", "session_stop",
    "status_change", "heartbeat", "fault_alert",
})
_SESSION_EVENTS = frozenset({"session_start", "meter_update", "session_stop"})
_REQUIRED_BASE  = ("event_id", "event_type", "timestamp", "station_id")


def validate(ev: dict) -> Optional[str]:
    """
    Return ``None`` if the event is valid, or a reason string if it should
    be rejected.  Reason strings are structured as ``<rule>:<value>`` so
    they can be parsed programmatically for alerting.
    """
    # Required base fields
    for f in _REQUIRED_BASE:
        if not ev.get(f):
            return f"missing_field:{f}"

    et = ev["event_type"]
    if et not in _VALID_EVENT_TYPES:
        return f"invalid_event_type:{et}"

    # Timestamp must be ISO-8601 parseable
    ts = ev["timestamp"]
    try:
        datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return f"invalid_timestamp:{ts!r}"

    # Session events require a session_id
    if et in _SESSION_EVENTS and not ev.get("session_id"):
        return "missing_field:session_id"

    # soc_percent: integer in [0, 100]
    soc = ev.get("soc_percent")
    if soc is not None:
        try:
            if not (0 <= int(soc) <= 100):
                return f"soc_out_of_range:{soc}"
        except (TypeError, ValueError):
            return f"invalid_soc_type:{soc!r}"

    # power_kw and energy_kwh: non-negative float
    for field in ("power_kw", "energy_kwh"):
        val = ev.get(field)
        if val is not None:
            try:
                if float(val) < 0:
                    return f"negative_{field}:{val}"
            except (TypeError, ValueError):
                return f"invalid_{field}_type:{val!r}"

    return None


class DeadLetterWriter:
    """
    Appends rejected events to a JSONL file.

    Thread-safe: ``open(..., 'a')`` is atomic on POSIX for writes smaller
    than PIPE_BUF (~4 KB), which covers every normal event record.
    """

    def __init__(self, path: str = DEAD_LETTER_PATH) -> None:
        self._path  = path
        self._count = 0
        log.info("Dead-letter sink: %s", os.path.abspath(path))

    def write(self, ev: object, reason: str) -> None:
        record = {
            "rejected_at": datetime.now(timezone.utc).isoformat(),
            "reason":      reason,
            "event":       ev,
        }
        try:
            with open(self._path, "a") as fh:
                fh.write(json.dumps(record, separators=(",", ":")) + "\n")
            self._count += 1
        except OSError as exc:
            log.error("Dead-letter write failed: %s", exc)

    @property
    def count(self) -> int:
        return self._count
