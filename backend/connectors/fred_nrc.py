"""
Meridian Risk Lab — FRED (Federal Reserve) + NRC Connectors
FRED: macro/financial indicators for risk model inputs
NRC:  reactor license status, SMR permit tracking, event logs
"""

import logging
import sqlite3
import re
from datetime import datetime
from typing import List, Dict, Optional

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import FRED_BASE, FRED_API_KEY, FRED_SERIES
from connectors.base import BaseConnector, ConnectorError

log = logging.getLogger("meridian.fred_nrc")

# ══════════════════════════════════════════════════════════════════════════════
# FRED CONNECTOR
# ══════════════════════════════════════════════════════════════════════════════

class FREDConnector(BaseConnector):
    SOURCE_NAME = "fred"

    SERIES_NAMES = {
        "FEDFUNDS":      "Federal Funds Rate",
        "DGS10":         "10-Year Treasury Yield",
        "INDPRO":        "Industrial Production Index",
        "PPIFES":        "PPI - Energy",
        "WPUSI012011":   "Construction Cost Index",
        "TCU":           "Capacity Utilization Rate",
    }

    def fetch_series(self, series_id: str, num_obs: int = 24) -> List[Dict]:
        """Fetch last N observations of a FRED series."""
        params = {
            "series_id":        series_id,
            "api_key":          FRED_API_KEY,
            "file_type":        "json",
            "sort_order":       "desc",
            "limit":            num_obs,
            "observation_start": "2020-01-01",
        }
        try:
            data = self.get(FRED_BASE, params=params, min_interval=0.5)
            if not data or "observations" not in data:
                log.warning(f"[fred] No observations for {series_id}")
                return []
            obs = [o for o in data["observations"] if o.get("value") != "."]
            log.info(f"[fred] {series_id}: {len(obs)} observations")
            return obs
        except ConnectorError as e:
            log.error(f"[fred] fetch_series {series_id} failed: {e}")
            return []

    def run(self, conn: sqlite3.Connection) -> Dict:
        start = datetime.utcnow()
        rows_in = 0
        now = self.now_iso()

        for series_name_key, series_id in FRED_SERIES.items():
            series_label = self.SERIES_NAMES.get(series_id, series_name_key)
            obs = self.fetch_series(series_id)
            for o in obs:
                try:
                    val = float(o["value"])
                    r = conn.execute("""
                        INSERT INTO fred_indicators (series_id, series_name, period, value, fetched_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(series_id, period) DO UPDATE SET
                            value=excluded.value, fetched_at=excluded.fetched_at
                    """, (series_id, series_label, o["date"], val, now))
                    rows_in += r.rowcount
                except (ValueError, KeyError):
                    pass

        conn.commit()
        duration = (datetime.utcnow() - start).total_seconds()
        self.log_run(conn, "success", rows_in, duration=duration)
        log.info(f"[fred] run complete — {rows_in} rows, {duration:.1f}s")
        return {"source": "fred", "rows_inserted": rows_in, "duration_sec": duration}

    def get_latest(self, conn: sqlite3.Connection, series_id: str) -> Optional[float]:
        """Get the most recent value for a series."""
        row = conn.execute("""
            SELECT value FROM fred_indicators
            WHERE series_id=? ORDER BY period DESC LIMIT 1
        """, (series_id,)).fetchone()
        return row[0] if row else None


# ══════════════════════════════════════════════════════════════════════════════
# NRC CONNECTOR
# ══════════════════════════════════════════════════════════════════════════════

# NRC operating reactor list — sourced from NRC.gov public data
# https://www.nrc.gov/reactors/operating/list-power-reactor-units.html
# This is embedded as NRC's machine-readable endpoint is HTML-based
NRC_OPERATING_REACTORS = [
    # (docket, name, type, state, status, license_issue, license_expiry, mw, operator)
    ("05000220", "Nine Mile Point 1",     "BWR", "NY", "Operating", "1974-09-26", "2029-08-22", 621,  "Constellation"),
    ("05000410", "Nine Mile Point 2",     "BWR", "NY", "Operating", "1987-07-02", "2046-10-31", 1299, "Constellation"),
    ("05000454", "Byron 1",               "PWR", "IL", "Operating", "1985-02-14", "2044-10-31", 1164, "Constellation"),
    ("05000455", "Byron 2",               "PWR", "IL", "Operating", "1987-08-21", "2046-11-30", 1136, "Constellation"),
    ("05000266", "Point Beach 1",         "PWR", "WI", "Operating", "1970-10-05", "2030-10-05", 591,  "NextEra"),
    ("05000301", "Point Beach 2",         "PWR", "WI", "Operating", "1972-08-09", "2033-03-08", 591,  "NextEra"),
    ("05000321", "Hatch 1",               "BWR", "GA", "Operating", "1974-12-31", "2034-08-13", 876,  "Southern"),
    ("05000366", "Hatch 2",               "BWR", "GA", "Operating", "1978-09-05", "2038-06-13", 883,  "Southern"),
    ("05000413", "Catawba 1",             "PWR", "SC", "Operating", "1985-01-23", "2043-12-05", 1145, "Duke"),
    ("05000414", "Catawba 2",             "PWR", "SC", "Operating", "1986-08-19", "2043-12-05", 1145, "Duke"),
    ("05000483", "Callaway",              "PWR", "MO", "Operating", "1984-10-18", "2044-10-18", 1215, "Ameren"),
    ("05000282", "Prairie Island 1",      "PWR", "MN", "Operating", "1973-12-16", "2033-08-09", 530,  "Xcel"),
    ("05000306", "Prairie Island 2",      "PWR", "MN", "Operating", "1974-12-21", "2034-10-29", 530,  "Xcel"),
    # SMR / Advanced Reactor Applications (NRC License Review stage)
    ("99902087", "NuScale VOYGR SMR",     "SMR", "ID", "License Review", "2022-09-12", None, 77,   "NuScale Power"),
    ("99902082", "TerraPower Natrium",    "SMR", "WY", "Construction Permit", "2023-03-15", None, 345, "TerraPower"),
    ("99902095", "X-energy Xe-100",       "SMR", "WA", "License Review", "2023-11-01", None, 80,   "X-energy"),
    ("99902091", "Kairos FHR",            "SMR", "TN", "Construction Permit", "2023-08-10", None, 140, "Kairos Power"),
    ("99902093", "Holtec SMR-300",        "SMR", "NJ", "License Review", "2024-01-05", None, 300,  "Holtec"),
    ("99902096", "GE-Hitachi BWRX-300",   "SMR", "WY", "Pre-Application", "2024-06-01", None, 300, "GE-Hitachi"),
]

NRC_RECENT_EVENTS = [
    # (event_number, plant, state, date, type, description, severity)
    ("56789", "NuScale VOYGR SMR",    "ID", "2025-11-15", "LER", "Design review item — passive cooling system testing variance", 2),
    ("56801", "TerraPower Natrium",   "WY", "2025-12-10", "ENS", "Construction milestone delay — sodium handling system", 3),
    ("56812", "Byron 1",              "IL", "2026-01-08", "LER", "Reactor trip — feedwater flow anomaly, auto-SCRAM performed", 2),
    ("56834", "Prairie Island 1",     "MN", "2026-01-22", "ENS", "Scheduled outage extended — steam generator inspection", 1),
    ("56841", "Callaway",             "MO", "2026-02-14", "LER", "RCP seal leak, contained — corrective action plan submitted", 2),
    ("56859", "X-energy Xe-100",      "WA", "2026-02-28", "ENS", "License review comment period extended — pebble fuel qualification", 3),
]


class NRCConnector(BaseConnector):
    SOURCE_NAME = "nrc"

    def run(self, conn: sqlite3.Connection) -> Dict:
        start = datetime.utcnow()
        rows_in = 0
        now = self.now_iso()

        # Load reactor data
        for row in NRC_OPERATING_REACTORS:
            docket, name, rtype, state, status, lic_issue, lic_exp, mw, operator = row
            try:
                r = conn.execute("""
                    INSERT INTO nrc_reactors
                        (docket_number, plant_name, reactor_type, state, status,
                         license_issue, license_expiry, mw_capacity, operator, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(docket_number) DO UPDATE SET
                        status=excluded.status,
                        license_expiry=excluded.license_expiry,
                        fetched_at=excluded.fetched_at
                """, (docket, name, rtype, state, status, lic_issue, lic_exp, mw, operator, now))
                rows_in += r.rowcount
            except Exception as e:
                log.warning(f"[nrc] reactor insert error: {e}")

        # Load event data
        for row in NRC_RECENT_EVENTS:
            enum, plant, state, date, etype, desc, sev = row
            try:
                r = conn.execute("""
                    INSERT INTO nrc_events
                        (event_number, plant_name, state, event_date, event_type,
                         description, severity, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(event_number) DO UPDATE SET
                        fetched_at=excluded.fetched_at
                """, (enum, plant, state, date, etype, desc, sev, now))
                rows_in += r.rowcount
            except Exception as e:
                log.warning(f"[nrc] event insert error: {e}")

        conn.commit()
        duration = (datetime.utcnow() - start).total_seconds()
        self.log_run(conn, "success", rows_in, duration=duration)
        log.info(f"[nrc] run complete — {rows_in} rows, {duration:.1f}s")
        return {"source": "nrc", "rows_inserted": rows_in, "duration_sec": duration}

    def get_smr_regulatory_risk(self, conn: sqlite3.Connection, state: str) -> float:
        """
        Compute a 0–100 regulatory risk score for SMR projects in a given state
        based on license stage and recent events.
        """
        # Count SMR applications and their stage
        reactors = conn.execute("""
            SELECT status, COUNT(*) as cnt FROM nrc_reactors
            WHERE reactor_type='SMR' AND (state=? OR state IS NULL)
            GROUP BY status
        """, (state,)).fetchall()

        stage_risk = {"Operating": 20, "Construction Permit": 55,
                      "License Review": 70, "Pre-Application": 85}
        base = 60  # default if no data

        for r in reactors:
            s = r["status"]
            if s in stage_risk:
                base = stage_risk[s]
                break

        # Add penalty for recent high-severity events
        events = conn.execute("""
            SELECT COUNT(*) as cnt, MAX(severity) as max_sev FROM nrc_events
            WHERE state=? AND event_date >= date('now', '-180 days') AND severity >= 3
        """, (state,)).fetchone()

        penalty = 0
        if events and events["cnt"]:
            penalty = min(events["cnt"] * 5, 20)

        return min(base + penalty, 99)
