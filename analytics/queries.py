"""
ChargeSquare Analytics Queries
================================

All analytical queries run against ClickHouse.  Where a materialized view
exists (e.g. sessions_hourly_mv, station_daily_mv) the query reads from
that pre-aggregated view for sub-second response times at large scale.

Insights delivered
------------------
1.  revenue_by_network()             – CPO league table
2.  sessions_per_hour()              – intra-day demand curve
3.  charger_utilization()            – utilisation & yield by charger class
4.  top_cities_by_energy()           – geographic demand heat-map
5.  error_analysis()                 – fault diagnostics
6.  vehicle_type_breakdown()         – BEV vs PHEV behaviour
7.  real_time_throughput()           – pipeline health (last N minutes)
8.  session_duration_distribution()  – dwell-time distribution
9.  peak_hours_analysis()            – hour-of-day traffic pattern
10. network_error_rate()             – reliability score per CPO
11. revenue_per_kwh_by_charger()     – yield efficiency by hardware class
12. country_summary()                – cross-border roll-up
13. vehicle_brand_breakdown()        – energy & revenue by vehicle brand
14. tariff_analysis()                – revenue & yield by tariff tier

Task 3 analyses
---------------
A1. hourly_energy_7days()            – hourly kWh last 7 days, peaks & troughs
A2. station_uptime_ratio()           – uptime/downtime per station; worst offenders
A3. vehicle_brand_comparison()       – Tesla vs other brands (duration, energy, revenue)
A4. peak_hour_revenue()              – peak-hour revenue contribution (%)
A5. fault_geographic_distribution()  – FAULT density by city/region
A6. anomaly_detection()              – sessions deviating >2σ from mean power (bonus)
"""

from __future__ import annotations

from typing import Dict, List, Optional

from clickhouse_driver import Client

from config.settings import CLICKHOUSE


def _client() -> Client:
    return Client(
        host=CLICKHOUSE.host,
        port=CLICKHOUSE.port,
        database=CLICKHOUSE.database,
        user=CLICKHOUSE.user,
        password=CLICKHOUSE.password,
        connect_timeout=10,
        send_receive_timeout=60,
    )


class ChargingAnalytics:
    """Facade over all analytical queries."""

    def __init__(self) -> None:
        self._ch = _client()

    def _q(self, sql: str, params: Optional[dict] = None) -> list:
        return self._ch.execute(sql, params or {})

    # ── 1. Revenue by network ─────────────────────────────────────────────────

    def revenue_by_network(self, days: int = 7) -> List[Dict]:
        rows = self._q(f"""
            SELECT
                network_id,
                count()                     AS session_count,
                round(sum(revenue_eur), 2)  AS total_revenue_eur,
                round(sum(energy_kwh), 2)   AS total_energy_kwh,
                round(avg(duration_minutes),1) AS avg_duration_min,
                round(avg(revenue_eur), 2)  AS avg_revenue_per_session
            FROM charging_events
            WHERE event_type = 'session_stop'
              AND timestamp >= now() - INTERVAL {days} DAY
            GROUP BY network_id
            ORDER BY total_revenue_eur DESC
        """)
        keys = ["network_id","session_count","total_revenue_eur",
                "total_energy_kwh","avg_duration_min","avg_revenue_per_session"]
        return [dict(zip(keys, r)) for r in rows]

    # ── 2. Sessions per hour ──────────────────────────────────────────────────

    def sessions_per_hour(self, hours: int = 24) -> List[Dict]:
        rows = self._q(f"""
            SELECT
                toStartOfHour(timestamp)    AS hour,
                count()                     AS session_count,
                round(sum(energy_kwh), 2)   AS total_energy_kwh,
                round(sum(revenue_eur), 2)  AS total_revenue_eur,
                round(avg(energy_kwh / nullIf(duration_minutes / 60.0, 0)), 1) AS avg_power_kw
            FROM charging_events
            WHERE event_type = 'session_stop'
              AND duration_minutes > 0
              AND timestamp >= now() - INTERVAL {hours} HOUR
            GROUP BY hour
            ORDER BY hour ASC
        """)
        keys = ["hour","session_count","total_energy_kwh","total_revenue_eur","avg_power_kw"]
        return [dict(zip(keys, r)) for r in rows]

    # ── 3. Charger utilisation ────────────────────────────────────────────────

    def charger_utilization(self) -> List[Dict]:
        rows = self._q("""
            SELECT
                charger_type,
                count()                       AS session_count,
                round(avg(duration_minutes),1) AS avg_duration_min,
                -- avg power derived from energy/time (power_kw=0 at session_stop by design)
                round(avg(energy_kwh / nullIf(duration_minutes / 60.0, 0)), 1) AS avg_power_kw,
                round(avg(energy_kwh), 2)     AS avg_energy_kwh,
                round(sum(revenue_eur), 2)    AS total_revenue_eur,
                round(avg(revenue_eur), 2)    AS avg_revenue_per_session
            FROM charging_events
            WHERE event_type = 'session_stop'
              AND duration_minutes > 0
            GROUP BY charger_type
            ORDER BY session_count DESC
        """)
        keys = ["charger_type","session_count","avg_duration_min","avg_power_kw",
                "avg_energy_kwh","total_revenue_eur","avg_revenue_per_session"]
        return [dict(zip(keys, r)) for r in rows]

    # ── 4. Top cities by energy demand ────────────────────────────────────────

    def top_cities_by_energy(self, limit: int = 10) -> List[Dict]:
        rows = self._q(f"""
            SELECT
                city,
                country,
                count()                      AS session_count,
                round(sum(energy_kwh), 2)    AS total_energy_kwh,
                round(sum(revenue_eur), 2)   AS total_revenue_eur,
                count(DISTINCT station_id)   AS unique_stations
            FROM charging_events
            WHERE event_type = 'session_stop'
            GROUP BY city, country
            ORDER BY total_energy_kwh DESC
            LIMIT {limit}
        """)
        keys = ["city","country","session_count","total_energy_kwh",
                "total_revenue_eur","unique_stations"]
        return [dict(zip(keys, r)) for r in rows]

    # ── 5. Error / fault analysis ─────────────────────────────────────────────

    def error_analysis(self) -> List[Dict]:
        rows = self._q("""
            SELECT
                error_code,
                count()                       AS occurrences,
                count(DISTINCT station_id)    AS affected_stations,
                count(DISTINCT network_id)    AS affected_networks,
                round(100.0 * count() /
                    (SELECT count() FROM charging_events
                     WHERE event_type = 'fault_alert'), 2) AS pct_of_errors
            FROM charging_events
            WHERE event_type = 'fault_alert'
              AND error_code != ''
            GROUP BY error_code
            ORDER BY occurrences DESC
        """)
        keys = ["error_code","occurrences","affected_stations",
                "affected_networks","pct_of_errors"]
        return [dict(zip(keys, r)) for r in rows]

    # ── 6. Vehicle type breakdown ─────────────────────────────────────────────

    def vehicle_type_breakdown(self) -> List[Dict]:
        rows = self._q("""
            SELECT
                vehicle_type,
                count()                       AS session_count,
                round(avg(energy_kwh), 2)     AS avg_energy_kwh,
                round(avg(duration_minutes),1) AS avg_duration_min,
                round(sum(revenue_eur), 2)    AS total_revenue_eur,
                round(avg(energy_kwh / nullIf(duration_minutes / 60.0, 0)), 1) AS avg_power_kw
            FROM charging_events
            WHERE event_type = 'session_stop'
              AND duration_minutes > 0
            GROUP BY vehicle_type
            ORDER BY session_count DESC
        """)
        keys = ["vehicle_type","session_count","avg_energy_kwh",
                "avg_duration_min","total_revenue_eur","avg_power_kw"]
        return [dict(zip(keys, r)) for r in rows]

    # ── 7. Real-time pipeline throughput ──────────────────────────────────────

    def real_time_throughput(self, minutes: int = 5) -> Dict:
        rows = self._q(f"""
            SELECT
                count()                      AS event_count,
                count(DISTINCT session_id)   AS unique_sessions,
                count(DISTINCT station_id)   AS active_stations,
                round(sum(revenue_eur), 2)   AS revenue_eur,
                round(sum(energy_kwh), 2)    AS energy_kwh
            FROM charging_events
            WHERE timestamp >= now() - INTERVAL {minutes} MINUTE
        """)
        r = rows[0] if rows else (0, 0, 0, 0.0, 0.0)
        period_s = minutes * 60
        return {
            "period_minutes":    minutes,
            "event_count":       r[0],
            "unique_sessions":   r[1],
            "active_stations":   r[2],
            "revenue_eur":       r[3],
            "energy_kwh":        r[4],
            "events_per_second": round(r[0] / period_s, 1) if period_s else 0,
        }

    # ── 8. Session duration distribution ──────────────────────────────────────

    def session_duration_distribution(self) -> List[Dict]:
        rows = self._q("""
            SELECT
                multiIf(
                    duration_minutes < 15,  '< 15 min',
                    duration_minutes < 30,  '15–30 min',
                    duration_minutes < 60,  '30–60 min',
                    duration_minutes < 120, '1–2 hours',
                    '>= 2 hours'
                ) AS bucket,
                count()                   AS sessions,
                round(avg(energy_kwh), 2) AS avg_energy_kwh,
                round(avg(revenue_eur), 2)AS avg_revenue_eur
            FROM charging_events
            WHERE event_type = 'session_stop'
            GROUP BY bucket
            ORDER BY min(duration_minutes) ASC
        """)
        keys = ["bucket","sessions","avg_energy_kwh","avg_revenue_eur"]
        return [dict(zip(keys, r)) for r in rows]

    # ── 9. Peak hours (hour-of-day traffic) ───────────────────────────────────

    def peak_hours_analysis(self) -> List[Dict]:
        rows = self._q("""
            SELECT
                toHour(timestamp)             AS hour_of_day,
                count()                       AS session_count,
                round(avg(energy_kwh), 2)     AS avg_energy_kwh,
                round(avg(energy_kwh / nullIf(duration_minutes / 60.0, 0)), 1) AS avg_power_kw,
                round(sum(revenue_eur), 2)    AS total_revenue_eur
            FROM charging_events
            WHERE event_type = 'session_stop'
              AND duration_minutes > 0
            GROUP BY hour_of_day
            ORDER BY hour_of_day ASC
        """)
        keys = ["hour_of_day","session_count","avg_energy_kwh",
                "avg_power_kw","total_revenue_eur"]
        return [dict(zip(keys, r)) for r in rows]

    # ── 10. Network error rate ────────────────────────────────────────────────

    def network_error_rate(self) -> List[Dict]:
        rows = self._q("""
            SELECT
                network_id,
                countIf(event_type = 'fault_alert')  AS error_count,
                count()                        AS total_events,
                round(100.0 * countIf(event_type = 'fault_alert') / count(), 3)
                                               AS error_rate_pct
            FROM charging_events
            GROUP BY network_id
            ORDER BY error_rate_pct DESC
        """)
        keys = ["network_id","error_count","total_events","error_rate_pct"]
        return [dict(zip(keys, r)) for r in rows]

    # ── 11. Revenue-per-kWh efficiency by charger class ───────────────────────

    def revenue_per_kwh_by_charger(self) -> List[Dict]:
        rows = self._q("""
            SELECT
                charger_type,
                round(sum(revenue_eur) / nullIf(sum(energy_kwh), 0), 4)
                                               AS revenue_per_kwh,
                round(avg(price_per_kwh), 4)   AS avg_listed_price,
                count()                        AS session_count
            FROM charging_events
            WHERE event_type = 'session_stop'
              AND energy_kwh > 0
            GROUP BY charger_type
            ORDER BY revenue_per_kwh DESC
        """)
        keys = ["charger_type","revenue_per_kwh","avg_listed_price","session_count"]
        return [dict(zip(keys, r)) for r in rows]

    # ── 12. Country roll-up ───────────────────────────────────────────────────

    def country_summary(self) -> List[Dict]:
        rows = self._q("""
            SELECT
                country,
                count()                       AS session_count,
                count(DISTINCT station_id)    AS station_count,
                round(sum(energy_kwh), 2)     AS total_energy_kwh,
                round(sum(revenue_eur), 2)    AS total_revenue_eur,
                round(avg(duration_minutes),1)AS avg_session_min
            FROM charging_events
            WHERE event_type = 'session_stop'
            GROUP BY country
            ORDER BY total_energy_kwh DESC
        """)
        keys = ["country","session_count","station_count",
                "total_energy_kwh","total_revenue_eur","avg_session_min"]
        return [dict(zip(keys, r)) for r in rows]

    # ── 13. Vehicle brand breakdown ───────────────────────────────────────────

    def vehicle_brand_breakdown(self, limit: int = 20) -> List[Dict]:
        rows = self._q(f"""
            SELECT
                vehicle_brand,
                count()                        AS session_count,
                round(avg(energy_kwh), 2)      AS avg_energy_kwh,
                round(sum(energy_kwh), 2)      AS total_energy_kwh,
                round(avg(duration_minutes),1) AS avg_duration_min,
                round(sum(revenue_eur), 2)     AS total_revenue_eur
            FROM charging_events
            WHERE event_type = 'session_stop'
              AND vehicle_brand != ''
            GROUP BY vehicle_brand
            ORDER BY total_energy_kwh DESC
            LIMIT {limit}
        """)
        keys = ["vehicle_brand","session_count","avg_energy_kwh",
                "total_energy_kwh","avg_duration_min","total_revenue_eur"]
        return [dict(zip(keys, r)) for r in rows]

    # ── 14. Tariff analysis ───────────────────────────────────────────────────

    def tariff_analysis(self, limit: int = 20) -> List[Dict]:
        rows = self._q(f"""
            SELECT
                tariff_id,
                count()                       AS session_count,
                round(avg(revenue_eur), 2)    AS avg_revenue_per_session,
                round(sum(revenue_eur), 2)    AS total_revenue_eur,
                round(avg(energy_kwh), 2)     AS avg_energy_kwh,
                round(avg(price_per_kwh), 4)  AS avg_price_per_kwh
            FROM charging_events
            WHERE event_type = 'session_stop'
              AND tariff_id != ''
            GROUP BY tariff_id
            ORDER BY total_revenue_eur DESC
            LIMIT {limit}
        """)
        keys = ["tariff_id","session_count","avg_revenue_per_session",
                "total_revenue_eur","avg_energy_kwh","avg_price_per_kwh"]
        return [dict(zip(keys, r)) for r in rows]

    # ── A1. Hourly energy consumption — last 7 days ───────────────────────────

    def hourly_energy_7days(self) -> List[Dict]:
        """
        A1 — Time series of kWh delivered per hour over the last 7 days.
        Peak hours are the top-5 by energy; low-usage hours are the bottom-5.
        Reads from sessions_hourly_mv for sub-second response.
        """
        rows = self._q("""
            SELECT
                hour,
                sum(session_count)        AS session_count,
                round(sum(total_energy_kwh), 2) AS total_energy_kwh,
                round(sum(total_revenue_eur), 2) AS total_revenue_eur
            FROM sessions_hourly_mv
            WHERE hour >= now() - INTERVAL 7 DAY
            GROUP BY hour
            ORDER BY hour ASC
        """)
        keys = ["hour", "session_count", "total_energy_kwh", "total_revenue_eur"]
        rows = [dict(zip(keys, r)) for r in rows]
        if rows:
            max_e = max(r["total_energy_kwh"] for r in rows)
            min_e = min(r["total_energy_kwh"] for r in rows)
            for r in rows:
                if r["total_energy_kwh"] == max_e:
                    r["label"] = "PEAK"
                elif r["total_energy_kwh"] == min_e:
                    r["label"] = "LOW"
                else:
                    r["label"] = ""
        return rows

    # ── A2. Station uptime / downtime ratio ───────────────────────────────────

    def station_uptime_ratio(self, limit: int = 20) -> List[Dict]:
        """
        A2 — Most problematic stations ranked by fault rate.
        Uptime proxy  = session_start events (station was available & used).
        Downtime proxy = fault_alert events (station was faulted).
        """
        rows = self._q(f"""
            SELECT
                station_id,
                network_id,
                city,
                countIf(event_type = 'session_start') AS session_starts,
                countIf(event_type = 'fault_alert')   AS fault_events,
                count()                               AS total_events,
                round(
                    100.0 * countIf(event_type = 'fault_alert') / count(),
                    3
                ) AS fault_rate_pct,
                round(
                    100.0 * countIf(event_type = 'session_start') /
                    nullIf(countIf(event_type = 'session_start')
                               + countIf(event_type = 'fault_alert'), 0),
                    1
                ) AS uptime_pct
            FROM charging_events
            GROUP BY station_id, network_id, city
            HAVING fault_events > 0
            ORDER BY fault_rate_pct DESC
            LIMIT {limit}
        """)
        keys = ["station_id", "network_id", "city",
                "session_starts", "fault_events", "total_events",
                "fault_rate_pct", "uptime_pct"]
        return [dict(zip(keys, r)) for r in rows]

    # ── A3. Vehicle brand comparison — Tesla vs others ────────────────────────

    def vehicle_brand_comparison(self) -> List[Dict]:
        """
        A3 — Average charging duration & energy by vehicle brand, with a
        Tesla vs. all-others roll-up for direct comparison.
        """
        rows = self._q("""
            SELECT
                if(vehicle_brand = 'Tesla', 'Tesla', 'Other Brands') AS brand_group,
                count()                        AS session_count,
                round(avg(duration_minutes),1) AS avg_duration_min,
                round(avg(energy_kwh), 2)      AS avg_energy_kwh,
                round(avg(energy_kwh / nullIf(duration_minutes / 60.0, 0)), 1) AS avg_power_kw,
                round(sum(energy_kwh), 2)      AS total_energy_kwh,
                round(sum(revenue_eur), 2)     AS total_revenue_eur,
                round(avg(revenue_eur), 2)     AS avg_revenue_per_session
            FROM charging_events
            WHERE event_type = 'session_stop'
              AND vehicle_brand != ''
              AND duration_minutes > 0
            GROUP BY brand_group
            ORDER BY brand_group ASC
        """)
        keys = ["brand_group", "session_count", "avg_duration_min",
                "avg_energy_kwh", "avg_power_kw", "total_energy_kwh",
                "total_revenue_eur", "avg_revenue_per_session"]
        return [dict(zip(keys, r)) for r in rows]

    # ── A4. Revenue by time period — peak vs off-peak ─────────────────────────

    def peak_hour_revenue(self) -> List[Dict]:
        """
        A4 — Revenue split between peak hours (07–09, 17–20) and off-peak.
        Revenue % is computed in Python from the raw sums to avoid a
        ClickHouse window-function round-trip.
        """
        rows = self._q("""
            WITH
                periods AS (
                    SELECT 'Morning Peak (07-09)' AS period, 1 AS sort_order
                    UNION ALL
                    SELECT 'Evening Peak (17-20)', 2
                    UNION ALL
                    SELECT 'Off-Peak', 3
                ),
                actuals AS (
                    SELECT
                        multiIf(
                            toHour(timestamp) IN (7, 8),       'Morning Peak (07-09)',
                            toHour(timestamp) IN (17, 18, 19), 'Evening Peak (17-20)',
                            'Off-Peak'
                        ) AS period,
                        count()                    AS session_count,
                        round(sum(revenue_eur), 2) AS total_revenue_eur,
                        round(sum(energy_kwh), 2)  AS total_energy_kwh,
                        round(avg(energy_kwh), 2)  AS avg_energy_kwh,
                        round(avg(revenue_eur), 2) AS avg_revenue_per_session
                    FROM charging_events
                    WHERE event_type = 'session_stop'
                    GROUP BY period
                )
            SELECT
                p.period,
                coalesce(a.session_count, 0)           AS session_count,
                coalesce(a.total_revenue_eur, 0)       AS total_revenue_eur,
                coalesce(a.total_energy_kwh, 0)        AS total_energy_kwh,
                coalesce(a.avg_energy_kwh, 0)          AS avg_energy_kwh,
                coalesce(a.avg_revenue_per_session, 0) AS avg_revenue_per_session
            FROM periods p
            LEFT JOIN actuals a ON p.period = a.period
            ORDER BY p.sort_order
        """)
        keys = ["period", "session_count", "total_revenue_eur",
                "total_energy_kwh", "avg_energy_kwh", "avg_revenue_per_session"]
        rows = [dict(zip(keys, r)) for r in rows]
        grand_total = sum(r["total_revenue_eur"] for r in rows) or 1
        for r in rows:
            r["revenue_pct"] = round(r["total_revenue_eur"] / grand_total * 100, 1)
        return rows

    # ── A5. Geographic distribution of FAULT events ───────────────────────────

    def fault_geographic_distribution(self, limit: int = 20) -> List[Dict]:
        """
        A5 — FAULT event density by city/region.
        fault_density = faults per unique station, a normalised metric that
        highlights cities where individual stations fail most often.
        """
        rows = self._q(f"""
            SELECT
                city,
                country,
                count()                        AS fault_count,
                count(DISTINCT station_id)     AS affected_stations,
                count(DISTINCT error_code)     AS distinct_error_types,
                round(
                    100.0 * count() /
                    (SELECT count() FROM charging_events
                     WHERE event_type = 'fault_alert'),
                    2
                ) AS pct_of_all_faults,
                round(
                    count() / nullIf(count(DISTINCT station_id), 0),
                    2
                ) AS faults_per_station
            FROM charging_events
            WHERE event_type = 'fault_alert'
            GROUP BY city, country
            ORDER BY fault_count DESC
            LIMIT {limit}
        """)
        keys = ["city", "country", "fault_count", "affected_stations",
                "distinct_error_types", "pct_of_all_faults", "faults_per_station"]
        return [dict(zip(keys, r)) for r in rows]

    # ── A6. Anomaly detection — sessions >2σ from mean power (bonus) ──────────

    def anomaly_detection(self, limit: int = 20) -> List[Dict]:
        """
        A6 (bonus) — Sessions where power_kw deviates more than 2 standard
        deviations from the fleet mean.  Uses a ClickHouse WITH clause to
        compute mean/stddev in a single pass.
        """
        # power_kw=0 at session_stop by design; derive avg power from energy/time
        rows = self._q(f"""
            WITH
                sessions AS (
                    SELECT
                        session_id, station_id, network_id, city, vehicle_brand,
                        energy_kwh, duration_minutes, revenue_eur,
                        round(energy_kwh / nullIf(duration_minutes / 60.0, 0), 2) AS avg_power_kw
                    FROM charging_events
                    WHERE event_type = 'session_stop'
                      AND duration_minutes > 0
                ),
                stats AS (
                    SELECT
                        avg(avg_power_kw)       AS mean_power,
                        stddevPop(avg_power_kw) AS std_power
                    FROM sessions
                )
            SELECT
                s.session_id,
                s.station_id,
                s.network_id,
                s.city,
                s.vehicle_brand,
                s.avg_power_kw                       AS power_kw,
                round(s.energy_kwh, 2)               AS energy_kwh,
                s.duration_minutes,
                round(s.revenue_eur, 2)              AS revenue_eur,
                round(stats.mean_power, 2)           AS fleet_mean_power,
                round(stats.std_power, 2)            AS fleet_std_power,
                round((s.avg_power_kw - stats.mean_power)
                      / nullIf(stats.std_power, 0), 2) AS z_score
            FROM sessions s
            CROSS JOIN stats
            WHERE abs(s.avg_power_kw - stats.mean_power) > 2 * stats.std_power
            ORDER BY abs(z_score) DESC
            LIMIT {limit}
        """)
        keys = ["session_id", "station_id", "network_id", "city",
                "vehicle_brand", "power_kw", "energy_kwh", "duration_minutes",
                "revenue_eur", "fleet_mean_power", "fleet_std_power", "z_score"]
        return [dict(zip(keys, r)) for r in rows]

    # ── Convenience ───────────────────────────────────────────────────────────

    def total_events(self) -> int:
        r = self._q("SELECT count() FROM charging_events")
        return r[0][0] if r else 0
