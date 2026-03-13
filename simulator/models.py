"""
Data models for EV charging events.

Using plain dataclasses (not Pydantic) in the hot-path to avoid per-object
validation overhead when generating millions of events per second.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# Enum-style string constants  (avoid enum overhead in tight loops)
# ─────────────────────────────────────────────────────────────────────────────

class EventType:
    SESSION_START  = "session_start"
    METER_UPDATE   = "meter_update"
    STATUS_CHANGE  = "status_change"
    SESSION_STOP   = "session_stop"
    HEARTBEAT      = "heartbeat"
    FAULT_ALERT    = "fault_alert"


class ChargerType:
    AC_LEVEL1      = "AC_LEVEL1"       # 3.7 kW  — slow home / destination
    AC_LEVEL2      = "AC_LEVEL2"       # 7–22 kW — standard public / workplace
    DC_FAST        = "DC_FAST"         # 50–150 kW — motorway / retail
    DC_ULTRA_FAST  = "DC_ULTRA_FAST"   # 150–350 kW — HPC / highway hubs


class VehicleType:
    BEV  = "BEV"   # Battery Electric Vehicle
    PHEV = "PHEV"  # Plug-in Hybrid Electric Vehicle


class StationStatus:
    AVAILABLE = "available"
    CHARGING  = "charging"
    FAULT     = "fault"
    OFFLINE   = "offline"
    RESERVED  = "reserved"


# ─────────────────────────────────────────────────────────────────────────────
# Core event model
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class ChargingEvent:
    """Represents a single event emitted by a charging station connector."""

    # Identity & routing
    event_id:   str
    event_type: str
    timestamp:  str   # ISO-8601 with UTC offset

    # Station topology
    station_id:   str
    connector_id: int
    session_id:   str
    network_id:   str

    # Location
    city:      str
    country:   str
    latitude:  float
    longitude: float

    # Hardware
    charger_type: str
    power_kw:     float

    # Session measurements
    energy_kwh:       float
    soc_percent:      int
    vehicle_type:     str
    duration_minutes: int

    # Commercial
    price_per_kwh: float
    revenue_eur:   float

    # Operational
    status:     str
    error_code: Optional[str]   = None
    component:  Optional[str]   = None

    # Electrical measurements (METER_UPDATE only)
    voltage_v:  Optional[float] = None
    current_a:  Optional[float] = None

    # Vehicle identity
    vehicle_brand: Optional[str] = None
    vehicle_model: Optional[str] = None
    vehicle_ev_id: Optional[str] = None

    # Tariff
    tariff_id: Optional[str] = None

    # ── serialisation ────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            "event_id":         self.event_id,
            "event_type":       self.event_type,
            "timestamp":        self.timestamp,
            "station_id":       self.station_id,
            "connector_id":     self.connector_id,
            "session_id":       self.session_id,
            "network_id":       self.network_id,
            "city":             self.city,
            "country":          self.country,
            "latitude":         self.latitude,
            "longitude":        self.longitude,
            "charger_type":     self.charger_type,
            "power_kw":         self.power_kw,
            "energy_kwh":       self.energy_kwh,
            "soc_percent":      self.soc_percent,
            "vehicle_type":     self.vehicle_type,
            "duration_minutes": self.duration_minutes,
            "price_per_kwh":    self.price_per_kwh,
            "revenue_eur":      self.revenue_eur,
            "status":           self.status,
            "error_code":       self.error_code,
            "component":        self.component,
            "voltage_v":        self.voltage_v,
            "current_a":        self.current_a,
            "vehicle_brand":    self.vehicle_brand,
            "vehicle_model":    self.vehicle_model,
            "vehicle_ev_id":    self.vehicle_ev_id,
            "tariff_id":        self.tariff_id,
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), separators=(",", ":")).encode("utf-8")
