"""
Meridian Risk Lab — EIA (Energy Information Administration) Connector
Pulls: electricity generation by fuel type, retail electricity prices by state.
Public API — free key at eia.gov (30 req/hr on DEMO_KEY).
Docs: https://api.eia.gov/v2/
"""

import logging
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import EIA_BASE, EIA_API_KEY
from connectors.base import BaseConnector, ConnectorError

log = logging.getLogger("meridian.eia")


class EIAConnector(BaseConnector):
    SOURCE_NAME = "eia"

    def fetch_generation(self, fuel_type: str, num_periods: int = 13) -> List[Dict]:
        """
        Fetch monthly electricity generation for a given fuel type (U.S. total).
        fuel_type: 'NG-NUC' | 'NG-GEO' | 'NG-OTH' (EIA facet codes)
        Returns list of {period, value, units} dicts.
        """
        # EIA v2 electricity/generation endpoint
        url = f"{EIA_BASE}/electricity/electric-power-operational-data/data/"
        params = {
            "api_key":          EIA_API_KEY,
            "frequency":        "monthly",
            "data[0]":          "generation",
            "facets[fueltypeid][]": fuel_type,
            "facets[location][]":   "US",
            "facets[sectorid][]":   "99",       # all sectors
            "sort[0][column]":  "period",
            "sort[0][direction]": "desc",
            "length":           num_periods,
            "offset":           0,
        }

        try:
            data = self.get(url, params=params, min_interval=1.5)
            if not data or "response" not in data:
                log.warning(f"[eia] No response data for fuel_type={fuel_type}")
                return []
            rows = data["response"].get("data", [])
            log.info(f"[eia] generation {fuel_type}: {len(rows)} periods")
            return rows
        except ConnectorError as e:
            log.error(f"[eia] fetch_generation failed: {e}")
            return []

    def fetch_state_prices(self, state: str, sector: str = "COM", num_periods: int = 13) -> List[Dict]:
        """
        Fetch monthly retail electricity prices for a given U.S. state.
        sector: 'RES' | 'COM' | 'IND'
        Returns list of {period, value, units} dicts — value in cents/kWh.
        """
        url = f"{EIA_BASE}/electricity/retail-sales/data/"
        params = {
            "api_key":              EIA_API_KEY,
            "frequency":            "monthly",
            "data[0]":              "price",
            "facets[stateid][]":    state,
            "facets[sectorid][]":   sector,
            "sort[0][column]":      "period",
            "sort[0][direction]":   "desc",
            "length":               num_periods,
        }

        try:
            data = self.get(url, params=params, min_interval=1.5)
            if not data or "response" not in data:
                return []
            rows = data["response"].get("data", [])
            log.info(f"[eia] prices {state}/{sector}: {len(rows)} periods")
            return rows
        except ConnectorError as e:
            log.error(f"[eia] fetch_state_prices failed {state}: {e}")
            return []

    def fetch_capacity(self, fuel_type: str) -> List[Dict]:
        """
        Fetch installed capacity (MW) for fuel type — useful for capacity factor calcs.
        """
        url = f"{EIA_BASE}/electricity/operating-generator-capacity/data/"
        params = {
            "api_key":              EIA_API_KEY,
            "frequency":            "annual",
            "data[0]":              "nameplate-capacity-mw",
            "facets[energy_source_code][]": fuel_type,
            "sort[0][column]":      "period",
            "sort[0][direction]":   "desc",
            "length":               5,
        }
        try:
            data = self.get(url, params=params, min_interval=1.5)
            if not data or "response" not in data:
                return []
            return data["response"].get("data", [])
        except ConnectorError as e:
            log.error(f"[eia] fetch_capacity failed: {e}")
            return []

    def run(self, conn: sqlite3.Connection) -> Dict:
        """Full EIA ETL pass — generation + key state prices."""
        start = datetime.utcnow()
        rows_in, rows_up = 0, 0
        now = self.now_iso()

        fuel_map = {
            "NUC": "nuclear",
            "GEO": "geothermal",
            "OTH": "storage",   # best EIA proxy for storage dispatch
        }

        # ── Generation ──────────────────────────────────────────────────────
        for fuel_code, label in fuel_map.items():
            rows = self.fetch_generation(fuel_type=fuel_code)
            for row in rows:
                try:
                    period = row.get("period", "")
                    val = row.get("generation")
                    if val is None:
                        continue
                    series_id = f"US-{fuel_code}-monthly"
                    r = conn.execute("""
                        INSERT INTO eia_generation (series_id, fuel_type, period, value_gwh, fetched_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(series_id, period) DO UPDATE SET
                            value_gwh=excluded.value_gwh,
                            fetched_at=excluded.fetched_at
                    """, (series_id, label, period, float(val), now))
                    if r.rowcount:
                        rows_in += 1
                except Exception as e:
                    log.warning(f"[eia] generation insert error: {e} row={row}")

        # ── State electricity prices — states with clean energy projects ────
        target_states = ["NV", "ID", "TX", "OR", "WY", "CA", "UT", "AZ", "NM", "CO"]
        for state in target_states:
            rows = self.fetch_state_prices(state, sector="COM")
            for row in rows:
                try:
                    val = row.get("price")
                    if val is None:
                        continue
                    r = conn.execute("""
                        INSERT INTO eia_prices (state, sector, period, price_cents_kwh, fetched_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(state, sector, period) DO UPDATE SET
                            price_cents_kwh=excluded.price_cents_kwh,
                            fetched_at=excluded.fetched_at
                    """, (state, "commercial", row.get("period", ""), float(val), now))
                    if r.rowcount:
                        rows_in += 1
                except Exception as e:
                    log.warning(f"[eia] price insert error {state}: {e}")

        conn.commit()
        duration = (datetime.utcnow() - start).total_seconds()
        self.log_run(conn, "success", rows_in, rows_up, duration=duration)
        log.info(f"[eia] run complete — {rows_in} rows, {duration:.1f}s")
        return {"source": "eia", "rows_inserted": rows_in, "duration_sec": duration}
