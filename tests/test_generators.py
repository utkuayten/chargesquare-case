"""Unit tests for the event generator."""

import json
import pytest

from simulator.models import EventType, ChargingEvent
from simulator.generators import EventGenerator, StationRegistry, NETWORKS, CITIES


@pytest.fixture(scope="module")
def registry():
    return StationRegistry(num_networks=3, stations_per_network=10, connectors_per_station=2)


@pytest.fixture(scope="module")
def generator(registry):
    return EventGenerator(registry)


class TestStationRegistry:
    def test_station_count(self, registry):
        assert len(registry.stations) == 30  # 3 networks × 10 stations

    def test_station_fields(self, registry):
        st = registry.random_station()
        required = {"station_id", "network_id", "city", "country",
                    "latitude", "longitude", "charger_type",
                    "power_min", "power_max", "price", "num_connectors"}
        assert required.issubset(st.keys())

    def test_station_id_format(self, registry):
        for st in registry.stations:
            # e.g. "IONITY_S00000"
            assert "_S" in st["station_id"]

    def test_price_range(self, registry):
        for st in registry.stations:
            assert 0.20 <= st["price"] <= 1.00

    def test_connector_count(self, registry):
        for st in registry.stations:
            assert st["num_connectors"] == 2


class TestEventGenerator:
    def test_batch_size(self, generator):
        batch = generator.generate_batch(100)
        assert len(batch) == 100

    def test_event_types_in_batch(self, generator):
        # Large batch should contain all high-frequency event types
        batch = generator.generate_batch(500)
        types = {ev.event_type for ev in batch}
        assert EventType.SESSION_START in types
        assert EventType.METER_UPDATE  in types
        assert EventType.SESSION_STOP  in types
        assert EventType.HEARTBEAT     in types

    def test_session_start_fields(self, generator):
        batch = generator.generate_batch(200)
        starts = [ev for ev in batch if ev.event_type == EventType.SESSION_START]
        assert starts, "No session_start events in 200-event batch"
        ev = starts[0]
        assert ev.energy_kwh == 0.0
        assert ev.duration_minutes == 0
        assert ev.revenue_eur == 0.0
        assert ev.soc_percent > 0

    def test_session_end_revenue(self, generator):
        batch = generator.generate_batch(500)
        ends = [ev for ev in batch if ev.event_type == EventType.SESSION_STOP]
        assert ends, "No session_stop events in 500-event batch"
        for ev in ends:
            if ev.energy_kwh > 0:
                assert ev.revenue_eur > 0
                assert ev.duration_minutes > 0

    def test_error_has_code(self, generator):
        # Generate until we get an error event
        for _ in range(10):
            batch = generator.generate_batch(500)
            errors = [ev for ev in batch if ev.event_type == EventType.FAULT_ALERT]
            if errors:
                for ev in errors:
                    assert ev.error_code is not None
                    assert ev.error_code != ""
                return
        pytest.skip("No error events generated in 5000 samples (low probability)")

    def test_json_serialisation(self, generator):
        batch = generator.generate_batch(50)
        for ev in batch:
            raw = ev.to_json()
            assert isinstance(raw, bytes)
            parsed = json.loads(raw)
            assert parsed["event_type"] == ev.event_type
            assert parsed["station_id"] == ev.station_id
            assert "timestamp" in parsed

    def test_to_dict_keys(self, generator):
        ev = generator.generate_batch(1)[0]
        d = ev.to_dict()
        expected_keys = {
            "event_id","event_type","timestamp","station_id","connector_id",
            "session_id","network_id","city","country","latitude","longitude",
            "charger_type","power_kw","energy_kwh","soc_percent","vehicle_type",
            "duration_minutes","price_per_kwh","revenue_eur","status",
            "error_code","component","voltage_v","current_a",
            "vehicle_brand","vehicle_model","vehicle_ev_id","tariff_id",
        }
        assert expected_keys == set(d.keys())

    def test_connector_id_range(self, generator):
        batch = generator.generate_batch(200)
        for ev in batch:
            assert 1 <= ev.connector_id <= 2  # registry has 2 connectors/station

    def test_power_within_charger_bounds(self, generator):
        batch = generator.generate_batch(200)
        for ev in batch:
            if ev.power_kw > 0:
                assert ev.power_kw >= 3.7   # AC Level 1 minimum
                assert ev.power_kw <= 360   # Ultra-fast maximum (+ small float margin)


class TestChargingEvent:
    def test_slots(self):
        # slots=True means no __dict__
        ev = ChargingEvent(
            event_id="id", event_type="test", timestamp="ts",
            station_id="s", connector_id=1, session_id="sid",
            network_id="n", city="c", country="DE",
            latitude=52.0, longitude=13.0,
            charger_type="DC_FAST", power_kw=100.0,
            energy_kwh=10.0, soc_percent=50, vehicle_type="BEV",
            duration_minutes=6, price_per_kwh=0.40, revenue_eur=4.0,
            status="charging",
        )
        assert not hasattr(ev, "__dict__")
