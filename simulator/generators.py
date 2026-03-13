"""
Event generator for the ChargeSquare EV charging simulator.

Produces realistic charging events drawn from weighted distributions
covering 10 European CPO networks, 20 cities, and four charger classes.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone
from typing import Dict, List

from .models import (
    ChargingEvent,
    ChargerType,
    EventType,
    StationStatus,
    VehicleType,
)


# ─────────────────────────────────────────────────────────────────────────────
# Reference data
# ─────────────────────────────────────────────────────────────────────────────

# Charging network operators (CPOs)
NETWORKS: List[Dict] = [
    {"id": "IONITY",       "price": 0.79},
    {"id": "TESLA_SC",     "price": 0.42},
    {"id": "CHARGEPOINT",  "price": 0.38},
    {"id": "EVBOX",        "price": 0.35},
    {"id": "ALLEGO",       "price": 0.41},
    {"id": "FASTNED",      "price": 0.69},
    {"id": "RECHARGE",     "price": 0.44},
    {"id": "SHELL_RC",     "price": 0.49},
    {"id": "BP_PULSE",     "price": 0.52},
    {"id": "VIRTA",        "price": 0.36},
]

# European cities — weight ≈ relative EV density / population
CITIES: List[Dict] = [
    {"city": "Berlin",       "country": "DE", "lat": 52.5200,  "lon": 13.4050,  "w": 10},
    {"city": "Hamburg",      "country": "DE", "lat": 53.5753,  "lon": 10.0153,  "w": 7},
    {"city": "Munich",       "country": "DE", "lat": 48.1351,  "lon": 11.5820,  "w": 8},
    {"city": "Frankfurt",    "country": "DE", "lat": 50.1109,  "lon": 8.6821,   "w": 7},
    {"city": "Amsterdam",    "country": "NL", "lat": 52.3676,  "lon": 4.9041,   "w": 9},
    {"city": "Rotterdam",    "country": "NL", "lat": 51.9244,  "lon": 4.4777,   "w": 5},
    {"city": "Paris",        "country": "FR", "lat": 48.8566,  "lon": 2.3522,   "w": 10},
    {"city": "Lyon",         "country": "FR", "lat": 45.7640,  "lon": 4.8357,   "w": 5},
    {"city": "London",       "country": "GB", "lat": 51.5074,  "lon": -0.1278,  "w": 10},
    {"city": "Manchester",   "country": "GB", "lat": 53.4808,  "lon": -2.2426,  "w": 6},
    {"city": "Oslo",         "country": "NO", "lat": 59.9139,  "lon": 10.7522,  "w": 8},
    {"city": "Bergen",       "country": "NO", "lat": 60.3913,  "lon": 5.3221,   "w": 4},
    {"city": "Stockholm",    "country": "SE", "lat": 59.3293,  "lon": 18.0686,  "w": 7},
    {"city": "Gothenburg",   "country": "SE", "lat": 57.7089,  "lon": 11.9746,  "w": 5},
    {"city": "Copenhagen",   "country": "DK", "lat": 55.6761,  "lon": 12.5683,  "w": 6},
    {"city": "Brussels",     "country": "BE", "lat": 50.8503,  "lon": 4.3517,   "w": 5},
    {"city": "Vienna",       "country": "AT", "lat": 48.2082,  "lon": 16.3738,  "w": 6},
    {"city": "Zurich",       "country": "CH", "lat": 47.3769,  "lon": 8.5417,   "w": 5},
    {"city": "Madrid",       "country": "ES", "lat": 40.4168,  "lon": -3.7038,  "w": 7},
    {"city": "Barcelona",    "country": "ES", "lat": 41.3851,  "lon": 2.1734,   "w": 6},
]

# Charger configs: (type, power_min_kw, power_max_kw, weight)
CHARGER_CONFIGS = [
    (ChargerType.AC_LEVEL1,     3.7,   7.4,  10),
    (ChargerType.AC_LEVEL2,     7.4,  22.0,  30),
    (ChargerType.DC_FAST,      50.0, 150.0,  40),
    (ChargerType.DC_ULTRA_FAST,150.0,350.0,  20),
]

VEHICLE_WEIGHTS = [
    (VehicleType.BEV,  75),
    (VehicleType.PHEV, 25),
]

# Vehicle catalog: brand → [(model, vehicle_type), ...]
VEHICLE_CATALOG: List[Dict] = [
    {"brand": "Tesla",      "model": "Model 3",         "vtype": VehicleType.BEV,  "w": 15},
    {"brand": "Tesla",      "model": "Model Y",         "vtype": VehicleType.BEV,  "w": 14},
    {"brand": "Tesla",      "model": "Model S",         "vtype": VehicleType.BEV,  "w": 5},
    {"brand": "Volkswagen", "model": "ID.4",            "vtype": VehicleType.BEV,  "w": 9},
    {"brand": "Volkswagen", "model": "ID.3",            "vtype": VehicleType.BEV,  "w": 7},
    {"brand": "BMW",        "model": "i4",              "vtype": VehicleType.BEV,  "w": 6},
    {"brand": "BMW",        "model": "iX",              "vtype": VehicleType.BEV,  "w": 4},
    {"brand": "Hyundai",    "model": "IONIQ 5",         "vtype": VehicleType.BEV,  "w": 7},
    {"brand": "Hyundai",    "model": "IONIQ 6",         "vtype": VehicleType.BEV,  "w": 5},
    {"brand": "Kia",        "model": "EV6",             "vtype": VehicleType.BEV,  "w": 6},
    {"brand": "Audi",       "model": "Q4 e-tron",       "vtype": VehicleType.BEV,  "w": 5},
    {"brand": "Mercedes",   "model": "EQS",             "vtype": VehicleType.BEV,  "w": 4},
    {"brand": "Mercedes",   "model": "EQA",             "vtype": VehicleType.BEV,  "w": 3},
    {"brand": "Renault",    "model": "Zoe",             "vtype": VehicleType.BEV,  "w": 5},
    {"brand": "Nissan",     "model": "Leaf",            "vtype": VehicleType.BEV,  "w": 4},
    {"brand": "Volvo",      "model": "XC40 Recharge",   "vtype": VehicleType.BEV,  "w": 4},
    {"brand": "Porsche",    "model": "Taycan",          "vtype": VehicleType.BEV,  "w": 3},
    {"brand": "Ford",       "model": "Mustang Mach-E",  "vtype": VehicleType.BEV,  "w": 3},
    {"brand": "BMW",        "model": "330e",            "vtype": VehicleType.PHEV, "w": 4},
    {"brand": "Volkswagen", "model": "Golf GTE",        "vtype": VehicleType.PHEV, "w": 3},
    {"brand": "Mercedes",   "model": "C 300 e",         "vtype": VehicleType.PHEV, "w": 3},
    {"brand": "Volvo",      "model": "XC60 Recharge",   "vtype": VehicleType.PHEV, "w": 3},
]

# Tariff IDs per network
NETWORK_TARIFFS: Dict[str, List[str]] = {
    "IONITY":      ["ionity-standard-v2", "ionity-peak-v2", "ionity-membership-v1"],
    "TESLA_SC":    ["tesla-sc-standard",  "tesla-sc-member"],
    "CHARGEPOINT": ["cp-standard-v1",     "cp-flex-v2",      "cp-overnight-v1"],
    "EVBOX":       ["evbox-standard",     "evbox-smart-v1"],
    "ALLEGO":      ["allego-standard-v1", "allego-peak-v1"],
    "FASTNED":     ["fastned-go",         "fastned-unlimited"],
    "RECHARGE":    ["recharge-standard",  "recharge-night-v1"],
    "SHELL_RC":    ["shell-standard-v1",  "shell-go+"],
    "BP_PULSE":    ["bp-standard",        "bp-plus-v1"],
    "VIRTA":       ["virta-standard-v1",  "virta-smart-v2"],
}

# Connector fault codes (OCPP-inspired)
ERROR_CODES = [
    "E001_GROUND_FAULT",
    "E002_OVERCURRENT",
    "E003_COMM_TIMEOUT",
    "E004_EMERGENCY_STOP",
    "E005_OVERVOLTAGE",
    "E006_CONNECTOR_LOCK",
    "E007_POWER_SUPPLY",
    "E008_OVERTEMPERATURE",
]

# OCPP-inspired component identifiers for fault alerts
FAULT_COMPONENTS = [
    "Connector",
    "PowerModule",
    "EnergyMeter",
    "ComputeUnit",
    "Display",
    "Controller",
    "CoolingSystem",
    "CableCheck",
]

# Event type distribution (spec-aligned frequencies)
EVENT_TYPE_WEIGHTS = [
    (EventType.SESSION_START,  8),
    (EventType.METER_UPDATE,  50),
    (EventType.STATUS_CHANGE, 12),
    (EventType.SESSION_STOP,  12),
    (EventType.HEARTBEAT,     15),
    (EventType.FAULT_ALERT,    3),
]

# Station status distribution
STATUS_WEIGHTS = [
    (StationStatus.AVAILABLE, 40),
    (StationStatus.CHARGING,  45),
    (StationStatus.OFFLINE,   10),
    (StationStatus.RESERVED,   5),
]


# ─────────────────────────────────────────────────────────────────────────────
# Station Registry — built once, shared across generator instances
# ─────────────────────────────────────────────────────────────────────────────

class StationRegistry:
    """Pre-generates the complete station universe to avoid per-event randomness."""

    def __init__(
        self,
        num_networks: int,
        stations_per_network: int,
        connectors_per_station: int,
    ) -> None:
        self.connectors_per_station = connectors_per_station
        self.stations: List[Dict] = []
        self._build(num_networks, stations_per_network)

    def _build(self, num_networks: int, stations_per_network: int) -> None:
        networks = NETWORKS[:num_networks]
        city_weights = [c["w"] for c in CITIES]
        ct_weights = [c[3] for c in CHARGER_CONFIGS]

        for network in networks:
            for i in range(stations_per_network):
                city = random.choices(CITIES, weights=city_weights, k=1)[0]
                ct_cfg = random.choices(CHARGER_CONFIGS, weights=ct_weights, k=1)[0]

                tariffs = NETWORK_TARIFFS.get(network["id"], ["standard-v1"])
                self.stations.append({
                    "station_id":   f"{network['id']}_S{i:05d}",
                    "network_id":   network["id"],
                    "city":         city["city"],
                    "country":      city["country"],
                    "latitude":     round(city["lat"] + random.uniform(-0.12, 0.12), 6),
                    "longitude":    round(city["lon"] + random.uniform(-0.12, 0.12), 6),
                    "charger_type": ct_cfg[0],
                    "power_min":    ct_cfg[1],
                    "power_max":    ct_cfg[2],
                    "price":        round(network["price"] + random.uniform(-0.04, 0.04), 4),
                    "num_connectors": self.connectors_per_station,
                    "tariffs":      tariffs,
                })

    def random_station(self) -> Dict:
        return random.choice(self.stations)


# ─────────────────────────────────────────────────────────────────────────────
# Event Generator
# ─────────────────────────────────────────────────────────────────────────────

class EventGenerator:
    """Generates realistic EV charging events using the station registry."""

    # Pre-computed weight lists for random.choices
    _ET_POPULATION  = [e for e, _ in EVENT_TYPE_WEIGHTS]
    _ET_WEIGHTS     = [w for _, w in EVENT_TYPE_WEIGHTS]
    _VT_POPULATION  = [v for v, _ in VEHICLE_WEIGHTS]
    _VT_WEIGHTS     = [w for _, w in VEHICLE_WEIGHTS]
    _ST_POPULATION  = [s for s, _ in STATUS_WEIGHTS]
    _ST_WEIGHTS     = [w for _, w in STATUS_WEIGHTS]
    _VC_POPULATION  = VEHICLE_CATALOG
    _VC_WEIGHTS     = [v["w"] for v in VEHICLE_CATALOG]

    def __init__(self, registry: StationRegistry) -> None:
        self.registry = registry

    # ── public API ────────────────────────────────────────────────────────────

    def generate_batch(self, size: int) -> List[ChargingEvent]:
        """Generate *size* events in one call."""
        # Pre-select event types for the whole batch in one random.choices call
        event_types = random.choices(
            self._ET_POPULATION, weights=self._ET_WEIGHTS, k=size
        )
        return [self._make_event(et) for et in event_types]

    # ── private helpers ───────────────────────────────────────────────────────

    def _make_event(self, event_type: str) -> ChargingEvent:
        if event_type == EventType.SESSION_START:
            return self._session_start()
        if event_type == EventType.METER_UPDATE:
            return self._meter_update()
        if event_type == EventType.SESSION_STOP:
            return self._session_stop()
        if event_type == EventType.FAULT_ALERT:
            return self._fault_alert()
        if event_type == EventType.HEARTBEAT:
            return self._heartbeat()
        return self._status_change()

    def _station_sample(self):
        """Return (station, connector_id, power_kw, price) tuple."""
        st = self.registry.random_station()
        cid = random.randint(1, st["num_connectors"])
        pwr = round(random.uniform(st["power_min"], st["power_max"]), 1)
        return st, cid, pwr

    def _vehicle_type(self) -> str:
        return random.choices(self._VT_POPULATION, weights=self._VT_WEIGHTS, k=1)[0]

    @staticmethod
    def _electrical(charger_type: str, power_kw: float):
        """Return (voltage_v, current_a) for a given charger type and power."""
        if charger_type == ChargerType.AC_LEVEL1:
            v = 230.0
        elif charger_type == ChargerType.AC_LEVEL2:
            v = 400.0
        elif charger_type == ChargerType.DC_FAST:
            v = round(random.uniform(400.0, 800.0), 1)
        else:  # DC_ULTRA_FAST
            v = round(random.uniform(800.0, 1000.0), 1)
        i = round(power_kw * 1000 / v, 1)
        return v, i

    def _vehicle(self) -> Dict:
        """Return a random vehicle entry from the catalog."""
        return random.choices(self._VC_POPULATION, weights=self._VC_WEIGHTS, k=1)[0]

    @staticmethod
    def _ev_id() -> str:
        """Generate a fake EV identifier (simplified VIN-like string)."""
        chars = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"
        return "EV" + "".join(random.choices(chars, k=15))

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _new_id() -> str:
        return str(uuid.uuid4())

    # ── event builders ────────────────────────────────────────────────────────

    def _session_start(self) -> ChargingEvent:
        st, cid, pwr = self._station_sample()
        veh = self._vehicle()
        return ChargingEvent(
            event_id=self._new_id(),
            event_type=EventType.SESSION_START,
            timestamp=self._now_iso(),
            station_id=st["station_id"],
            connector_id=cid,
            session_id=self._new_id(),
            network_id=st["network_id"],
            city=st["city"],
            country=st["country"],
            latitude=st["latitude"],
            longitude=st["longitude"],
            charger_type=st["charger_type"],
            power_kw=pwr,
            energy_kwh=0.0,
            soc_percent=random.randint(5, 50),
            vehicle_type=veh["vtype"],
            duration_minutes=0,
            price_per_kwh=st["price"],
            revenue_eur=0.0,
            status=StationStatus.CHARGING,
            vehicle_brand=veh["brand"],
            vehicle_model=veh["model"],
            vehicle_ev_id=self._ev_id(),
            tariff_id=random.choice(st["tariffs"]),
        )

    def _meter_update(self) -> ChargingEvent:
        st, cid, pwr = self._station_sample()
        dur = random.randint(5, 60)
        energy = round(pwr * dur / 60 * random.uniform(0.85, 0.95), 2)
        voltage, current = self._electrical(st["charger_type"], pwr)
        veh = self._vehicle()
        return ChargingEvent(
            event_id=self._new_id(),
            event_type=EventType.METER_UPDATE,
            timestamp=self._now_iso(),
            station_id=st["station_id"],
            connector_id=cid,
            session_id=self._new_id(),
            network_id=st["network_id"],
            city=st["city"],
            country=st["country"],
            latitude=st["latitude"],
            longitude=st["longitude"],
            charger_type=st["charger_type"],
            power_kw=pwr,
            energy_kwh=energy,
            soc_percent=random.randint(30, 90),
            vehicle_type=veh["vtype"],
            duration_minutes=dur,
            price_per_kwh=st["price"],
            revenue_eur=round(energy * st["price"], 2),
            status=StationStatus.CHARGING,
            voltage_v=voltage,
            current_a=current,
            vehicle_brand=veh["brand"],
            vehicle_model=veh["model"],
            vehicle_ev_id=self._ev_id(),
            tariff_id=random.choice(st["tariffs"]),
        )

    def _session_stop(self) -> ChargingEvent:
        st, cid, pwr = self._station_sample()
        dur = random.randint(10, 120)
        energy = round(pwr * dur / 60 * random.uniform(0.85, 0.95), 2)
        veh = self._vehicle()
        return ChargingEvent(
            event_id=self._new_id(),
            event_type=EventType.SESSION_STOP,
            timestamp=self._now_iso(),
            station_id=st["station_id"],
            connector_id=cid,
            session_id=self._new_id(),
            network_id=st["network_id"],
            city=st["city"],
            country=st["country"],
            latitude=st["latitude"],
            longitude=st["longitude"],
            charger_type=st["charger_type"],
            power_kw=pwr,
            energy_kwh=energy,
            soc_percent=random.randint(60, 100),
            vehicle_type=veh["vtype"],
            duration_minutes=dur,
            price_per_kwh=st["price"],
            revenue_eur=round(energy * st["price"], 2),
            status=StationStatus.AVAILABLE,
            vehicle_brand=veh["brand"],
            vehicle_model=veh["model"],
            vehicle_ev_id=self._ev_id(),
            tariff_id=random.choice(st["tariffs"]),
        )

    def _fault_alert(self) -> ChargingEvent:
        st, cid, _ = self._station_sample()
        return ChargingEvent(
            event_id=self._new_id(),
            event_type=EventType.FAULT_ALERT,
            timestamp=self._now_iso(),
            station_id=st["station_id"],
            connector_id=cid,
            session_id=self._new_id(),
            network_id=st["network_id"],
            city=st["city"],
            country=st["country"],
            latitude=st["latitude"],
            longitude=st["longitude"],
            charger_type=st["charger_type"],
            power_kw=0.0,
            energy_kwh=0.0,
            soc_percent=0,
            vehicle_type=VehicleType.BEV,
            duration_minutes=0,
            price_per_kwh=st["price"],
            revenue_eur=0.0,
            status=StationStatus.FAULT,
            error_code=random.choice(ERROR_CODES),
            component=random.choice(FAULT_COMPONENTS),
        )

    def _status_change(self) -> ChargingEvent:
        st, cid, pwr = self._station_sample()
        status = random.choices(self._ST_POPULATION, weights=self._ST_WEIGHTS, k=1)[0]
        return ChargingEvent(
            event_id=self._new_id(),
            event_type=EventType.STATUS_CHANGE,
            timestamp=self._now_iso(),
            station_id=st["station_id"],
            connector_id=cid,
            session_id="",
            network_id=st["network_id"],
            city=st["city"],
            country=st["country"],
            latitude=st["latitude"],
            longitude=st["longitude"],
            charger_type=st["charger_type"],
            power_kw=pwr if status == StationStatus.CHARGING else 0.0,
            energy_kwh=0.0,
            soc_percent=0,
            vehicle_type=VehicleType.BEV,
            duration_minutes=0,
            price_per_kwh=st["price"],
            revenue_eur=0.0,
            status=status,
        )

    def _heartbeat(self) -> ChargingEvent:
        st, cid, _ = self._station_sample()
        return ChargingEvent(
            event_id=self._new_id(),
            event_type=EventType.HEARTBEAT,
            timestamp=self._now_iso(),
            station_id=st["station_id"],
            connector_id=cid,
            session_id="",
            network_id=st["network_id"],
            city=st["city"],
            country=st["country"],
            latitude=st["latitude"],
            longitude=st["longitude"],
            charger_type=st["charger_type"],
            power_kw=0.0,
            energy_kwh=0.0,
            soc_percent=0,
            vehicle_type="",
            duration_minutes=0,
            price_per_kwh=0.0,
            revenue_eur=0.0,
            status=StationStatus.AVAILABLE,
        )
