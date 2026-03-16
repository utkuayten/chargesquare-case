"""
Session Lifecycle Manager
=========================

Generates realistic EV charging event streams where every SESSION_START
is followed by consecutive METER_UPDATEs and a final SESSION_STOP.

Session lifecycle
-----------------
  SESSION_START
    METER_UPDATE  ← energy & SoC advance monotonically
    METER_UPDATE
    ...           ← 3–15 updates (configurable)
    METER_UPDATE
  SESSION_STOP

Background events (HEARTBEAT, STATUS_CHANGE, FAULT_ALERT) fill the gaps
between session lifecycles so the target EPS is maintained.

Peak-hour traffic shaping
--------------------------
The probability of opening a new session is multiplied by
`peak_multiplier` during the configured UTC hour windows, producing a
realistic intra-day demand curve with morning and evening peaks.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone
from typing import List

from .generators import (
    CITIES, CHARGER_CONFIGS, ERROR_CODES, FAULT_COMPONENTS,
    NETWORK_TARIFFS, VEHICLE_CATALOG, EventGenerator,
)
from .models import ChargingEvent, ChargerType, EventType, StationStatus
from config.settings import SimulatorConfig


class SessionManager:
    """
    Stateless batch generator that produces coherent session lifecycles.

    Each call to ``generate_batch(n)`` returns *n* events that include:
      - Complete session lifecycles:  START + N×METER_UPDATE + STOP
      - Background noise:             HEARTBEAT, STATUS_CHANGE, FAULT_ALERT

    The mix is controlled by ``session_fraction`` and the current
    peak-hour multiplier.
    """

    _VC_POPULATION = VEHICLE_CATALOG
    _VC_WEIGHTS    = [v["w"] for v in VEHICLE_CATALOG]

    def __init__(self, registry, cfg: SimulatorConfig, clock_fn=None) -> None:
        self._registry = registry
        self._cfg      = cfg
        self._clock_fn = clock_fn  # optional: callable() -> datetime (for backfill)

    # ── public API ────────────────────────────────────────────────────────────

    def generate_batch(self, size: int) -> List[ChargingEvent]:
        """
        Return exactly *size* events.

        Session lifecycles are generated first; background events fill
        any remaining quota.
        """
        peak_mult  = self._peak_multiplier()
        frac       = min(self._cfg.session_fraction * peak_mult, 0.85)
        session_budget = int(size * frac)

        events: List[ChargingEvent] = []

        # ── session lifecycles ────────────────────────────────────────────────
        while len(events) < session_budget:
            events.extend(self._session_lifecycle())

        # ── background events ─────────────────────────────────────────────────
        while len(events) < size:
            events.append(self._background_event())

        return events[:size]

    # ── peak-hour logic ───────────────────────────────────────────────────────

    def _peak_multiplier(self) -> float:
        now = self._clock_fn() if self._clock_fn else datetime.now(timezone.utc)
        h = now.hour
        for window in self._cfg.peak_hours:
            start, end = window[0], window[1]
            mult = window[2] if len(window) > 2 else self._cfg.peak_multiplier
            if start <= h < end:
                return mult
        return 1.0

    # ── session lifecycle builder ─────────────────────────────────────────────

    def _ts(self) -> str:
        if self._clock_fn:
            return self._clock_fn().isoformat()
        return _now()

    def _session_lifecycle(self) -> List[ChargingEvent]:
        """
        Build a complete session:  START  +  N × METER_UPDATE  +  STOP.
        All events share the same session_id, station, vehicle and tariff.
        """
        cfg = self._cfg
        st  = self._registry.random_station()
        cid = random.randint(1, st["num_connectors"])
        pwr = round(random.uniform(st["power_min"], st["power_max"]), 1)
        veh = random.choices(self._VC_POPULATION, weights=self._VC_WEIGHTS, k=1)[0]

        session_id  = str(uuid.uuid4())
        tariff_id   = random.choice(st["tariffs"])
        price       = st["price"]
        n_updates   = random.randint(cfg.meter_updates_min, cfg.meter_updates_max)
        duration    = random.randint(cfg.session_duration_min, cfg.session_duration_max)
        soc_start   = random.randint(*cfg.soc_start_range)
        soc_end     = random.randint(*cfg.soc_end_range)
        energy_step = round(pwr * (duration / n_updates) / 60 * random.uniform(0.85, 0.95), 3)
        voltage, current = EventGenerator._electrical(st["charger_type"], pwr)

        common = dict(
            station_id=st["station_id"],
            connector_id=cid,
            session_id=session_id,
            network_id=st["network_id"],
            city=st["city"], country=st["country"],
            latitude=st["latitude"], longitude=st["longitude"],
            charger_type=st["charger_type"],
            vehicle_type=veh["vtype"],
            vehicle_brand=veh["brand"],
            vehicle_model=veh["model"],
            vehicle_ev_id=_ev_id(),
            tariff_id=tariff_id,
            price_per_kwh=price,
        )

        events: List[ChargingEvent] = []

        # SESSION_START
        events.append(ChargingEvent(
            event_id=_new_id(), event_type=EventType.SESSION_START,
            timestamp=self._ts(),
            power_kw=pwr, energy_kwh=0.0,
            soc_percent=soc_start, duration_minutes=0,
            revenue_eur=0.0, status=StationStatus.CHARGING,
            **common,
        ))

        # METER_UPDATEs  — energy and SoC advance monotonically
        for i in range(1, n_updates + 1):
            cumulative  = round(energy_step * i, 2)
            soc_now     = soc_start + int((soc_end - soc_start) * i / n_updates)
            dur_now     = int(duration * i / n_updates)
            events.append(ChargingEvent(
                event_id=_new_id(), event_type=EventType.METER_UPDATE,
                timestamp=self._ts(),
                power_kw=pwr, energy_kwh=cumulative,
                soc_percent=min(100, soc_now), duration_minutes=dur_now,
                revenue_eur=round(cumulative * price, 2),
                status=StationStatus.CHARGING,
                voltage_v=voltage, current_a=current,
                **common,
            ))

        # SESSION_STOP
        total_energy = round(energy_step * n_updates, 2)
        events.append(ChargingEvent(
            event_id=_new_id(), event_type=EventType.SESSION_STOP,
            timestamp=self._ts(),
            power_kw=0.0, energy_kwh=total_energy,
            soc_percent=min(100, soc_end), duration_minutes=duration,
            revenue_eur=round(total_energy * price, 2),
            status=StationStatus.AVAILABLE,
            **common,
        ))

        return events

    # ── background events ─────────────────────────────────────────────────────

    def _background_event(self) -> ChargingEvent:
        """
        Emit a non-session event.  Fault probability is controlled by
        ``fault_rate_pct``; remainder is split evenly between HEARTBEAT
        and STATUS_CHANGE.
        """
        roll = random.random() * 100
        if roll < self._cfg.fault_rate_pct:
            return self._fault_alert()
        if roll < self._cfg.fault_rate_pct + 40:
            return self._heartbeat()
        return self._status_change()

    def _fault_alert(self) -> ChargingEvent:
        st, cid = self._station_sample_simple()
        return ChargingEvent(
            event_id=_new_id(), event_type=EventType.FAULT_ALERT,
            timestamp=self._ts(),
            station_id=st["station_id"], connector_id=cid,
            session_id="", network_id=st["network_id"],
            city=st["city"], country=st["country"],
            latitude=st["latitude"], longitude=st["longitude"],
            charger_type=st["charger_type"],
            power_kw=0.0, energy_kwh=0.0, soc_percent=0,
            vehicle_type="", duration_minutes=0,
            price_per_kwh=st["price"], revenue_eur=0.0,
            status=StationStatus.FAULT,
            error_code=random.choice(ERROR_CODES),
            component=random.choice(FAULT_COMPONENTS),
        )

    def _heartbeat(self) -> ChargingEvent:
        st, cid = self._station_sample_simple()
        return ChargingEvent(
            event_id=_new_id(), event_type=EventType.HEARTBEAT,
            timestamp=self._ts(),
            station_id=st["station_id"], connector_id=cid,
            session_id="", network_id=st["network_id"],
            city=st["city"], country=st["country"],
            latitude=st["latitude"], longitude=st["longitude"],
            charger_type=st["charger_type"],
            power_kw=0.0, energy_kwh=0.0, soc_percent=0,
            vehicle_type="", duration_minutes=0,
            price_per_kwh=0.0, revenue_eur=0.0,
            status=StationStatus.AVAILABLE,
        )

    def _status_change(self) -> ChargingEvent:
        st, cid = self._station_sample_simple()
        status = random.choice([
            StationStatus.AVAILABLE, StationStatus.AVAILABLE,
            StationStatus.OFFLINE, StationStatus.RESERVED,
        ])
        return ChargingEvent(
            event_id=_new_id(), event_type=EventType.STATUS_CHANGE,
            timestamp=self._ts(),
            station_id=st["station_id"], connector_id=cid,
            session_id="", network_id=st["network_id"],
            city=st["city"], country=st["country"],
            latitude=st["latitude"], longitude=st["longitude"],
            charger_type=st["charger_type"],
            power_kw=0.0, energy_kwh=0.0, soc_percent=0,
            vehicle_type="", duration_minutes=0,
            price_per_kwh=0.0, revenue_eur=0.0,
            status=status,
        )

    def _station_sample_simple(self):
        st  = self._registry.random_station()
        cid = random.randint(1, st["num_connectors"])
        return st, cid


# ─────────────────────────────────────────────────────────────────────────────
# Tiny helpers (module-level to avoid per-call attribute lookups)
# ─────────────────────────────────────────────────────────────────────────────

def _new_id() -> str:
    return str(uuid.uuid4())

def _ev_id() -> str:
    chars = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"
    return "EV" + "".join(random.choices(chars, k=15))

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
