"""Unit tests for the ClickHouse consumer row converter."""

from datetime import datetime, timezone

import pytest

from consumers.clickhouse_consumer import event_to_row, _parse_ts


class TestParseTimestamp:
    def test_iso_utc(self):
        ts = "2024-06-15T14:30:00+00:00"
        dt = _parse_ts(ts)
        assert isinstance(dt, datetime)
        assert dt.year == 2024
        assert dt.month == 6

    def test_z_suffix(self):
        ts = "2024-01-01T00:00:00Z"
        dt = _parse_ts(ts)
        assert dt.year == 2024

    def test_none_returns_now(self):
        dt = _parse_ts(None)
        assert isinstance(dt, datetime)
        # Should be close to now
        diff = abs((datetime.now(timezone.utc) - dt).total_seconds())
        assert diff < 5

    def test_invalid_returns_now(self):
        dt = _parse_ts("not-a-date")
        assert isinstance(dt, datetime)


class TestEventToRow:
    def _sample_event(self, overrides=None):
        ev = {
            "event_id":         "abc-123",
            "event_type":       "session_end",
            "timestamp":        "2024-06-15T10:00:00Z",
            "station_id":       "IONITY_S00001",
            "connector_id":     "2",
            "session_id":       "sess-456",
            "network_id":       "IONITY",
            "city":             "Berlin",
            "country":          "DE",
            "latitude":         "52.52",
            "longitude":        "13.41",
            "charger_type":     "DC_FAST",
            "power_kw":         "150.0",
            "energy_kwh":       "45.0",
            "soc_percent":      "80",
            "vehicle_type":     "BEV",
            "duration_minutes": "30",
            "price_per_kwh":    "0.79",
            "revenue_eur":      "35.55",
            "status":           "available",
            "error_code":       None,
        }
        if overrides:
            ev.update(overrides)
        return ev

    def test_row_length(self):
        row = event_to_row(self._sample_event())
        assert len(row) == 21

    def test_types(self):
        row = event_to_row(self._sample_event())
        event_id, event_type, ts = row[0], row[1], row[2]
        assert isinstance(event_id, str)
        assert isinstance(event_type, str)
        assert isinstance(ts, datetime)

    def test_numeric_conversion(self):
        row = event_to_row(self._sample_event())
        connector_id     = row[4]
        power_kw         = row[12]
        energy_kwh       = row[13]
        soc_percent      = row[14]
        duration_minutes = row[16]
        revenue_eur      = row[18]

        assert isinstance(connector_id, int)
        assert isinstance(power_kw, float)
        assert isinstance(energy_kwh, float)
        assert isinstance(soc_percent, int)
        assert isinstance(duration_minutes, int)
        assert isinstance(revenue_eur, float)

    def test_error_code_none_becomes_empty_string(self):
        row = event_to_row(self._sample_event({"error_code": None}))
        assert row[20] == ""

    def test_error_code_preserved(self):
        row = event_to_row(self._sample_event({"error_code": "E001_GROUND_FAULT"}))
        assert row[20] == "E001_GROUND_FAULT"

    def test_missing_fields_use_defaults(self):
        # Minimal event — all optional fields missing
        row = event_to_row({"event_type": "error"})
        assert row[0] == ""     # event_id
        assert row[4] == 0      # connector_id
        assert row[12] == 0.0   # power_kw
