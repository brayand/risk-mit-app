"""
Meridian Risk Lab — USGS Seismic Connector
Pulls: earthquake events (M≥2.5, U.S., last 90 days) + per-state seismic risk summaries.
Public API — https://earthquake.usgs.gov/fdsnws/event/1/
Relevant for: Geothermal (induced seismicity), Nuclear SMR (site suitability), Storage (site).
"""

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import USGS_EARTHQUAKE, USGS_EQ_PARAMS, STATE_SEISMIC_ZONE
from connectors.base import BaseConnector, ConnectorError

log = logging.getLogger("meridian.usgs")

# State bounding boxes for approximate state assignment from lat/lon
STATE_BOUNDS = {
    "AK": (54.0,  71.5,  -168.0, -130.0),
    "AL": (30.1,  35.0,   -88.5,  -84.9),
    "AR": (33.0,  36.5,   -94.6,  -89.6),
    "AZ": (31.3,  37.0,  -114.8, -109.0),
    "CA": (32.5,  42.0,  -124.5, -114.1),
    "CO": (37.0,  41.0,  -109.1, -102.0),
    "CT": (40.9,  42.1,   -73.7,  -71.8),
    "DE": (38.4,  39.8,   -75.8,  -75.0),
    "FL": (24.4,  31.0,   -87.6,  -80.0),
    "GA": (30.4,  35.0,   -85.6,  -81.0),
    "HI": (18.9,  22.2,  -160.2, -154.8),
    "IA": (40.4,  43.5,   -96.6,  -90.1),
    "ID": (42.0,  49.0,  -117.2, -111.0),
    "IL": (37.0,  42.5,   -91.5,  -87.5),
    "IN": (37.8,  41.8,   -88.1,  -84.8),
    "KS": (37.0,  40.0,  -102.1,  -94.6),
    "KY": (36.5,  39.1,   -89.6,  -81.9),
    "LA": (29.0,  33.0,   -94.1,  -88.8),
    "MA": (41.2,  42.9,   -73.5,  -70.0),
    "MD": (37.9,  39.7,   -79.5,  -75.1),
    "ME": (43.0,  47.5,   -71.1,  -67.0),
    "MI": (41.7,  48.3,   -90.4,  -82.4),
    "MN": (43.5,  49.4,   -97.2,  -89.5),
    "MO": (36.0,  40.6,   -95.8,  -89.1),
    "MS": (30.2,  35.0,   -91.7,  -88.1),
    "MT": (44.4,  49.0,  -116.1, -104.0),
    "NC": (33.8,  36.6,   -84.3,  -75.5),
    "ND": (45.9,  49.0,  -104.1,  -96.6),
    "NE": (40.0,  43.0,  -104.1,  -95.3),
    "NH": (42.7,  45.3,   -72.6,  -70.7),
    "NJ": (38.9,  41.4,   -75.6,  -73.9),
    "NM": (31.3,  37.0,  -109.1, -103.0),
    "NV": (35.0,  42.0,  -120.0, -114.0),
    "NY": (40.5,  45.0,   -79.8,  -71.9),
    "OH": (38.4,  42.3,   -84.8,  -80.5),
    "OK": (33.6,  37.0,  -103.0,  -94.4),
    "OR": (42.0,  46.2,  -124.6, -116.5),
    "PA": (39.7,  42.3,   -80.5,  -74.7),
    "RI": (41.1,  42.0,   -71.9,  -71.1),
    "SC": (32.0,  35.2,   -83.4,  -78.5),
    "SD": (42.5,  45.9,  -104.1,  -96.4),
    "TN": (35.0,  36.7,   -90.3,  -81.6),
    "TX": (25.8,  36.5,  -106.6,  -93.5),
    "UT": (37.0,  42.0,  -114.1, -109.0),
    "VA": (36.5,  39.5,   -83.7,  -75.2),
    "VT": (42.7,  45.0,   -73.5,  -71.5),
    "WA": (45.5,  49.0,  -124.8, -116.9),
    "WI": (42.5,  47.1,   -92.9,  -86.2),
    "WV": (37.2,  40.6,   -82.6,  -77.7),
    "WY": (41.0,  45.0,  -111.1, -104.1),
}


def lat_lon_to_state(lat: float, lon: float) -> Optional[str]:
    """Approximate state lookup from lat/lon using bounding boxes."""
    for state, (min_lat, max_lat, min_lon, max_lon) in STATE_BOUNDS.items():
        if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
            return state
    return None


class USGSConnector(BaseConnector):
    SOURCE_NAME = "usgs"

    def fetch_earthquakes(self, days_back: int = 90, min_magnitude: float = 2.5) -> List[Dict]:
        """
        Fetch U.S. earthquake events from USGS FDSN API.
        Returns list of event dicts.
        """
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=days_back)

        params = {
            **USGS_EQ_PARAMS,
            "starttime":        start_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "endtime":          end_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "minmagnitude":     min_magnitude,
            "orderby":          "time",
            "limit":            2000,
        }

        try:
            data = self.get(USGS_EARTHQUAKE, params=params, min_interval=1.0)
            if not data or "features" not in data:
                log.warning("[usgs] No earthquake features in response")
                return []
            features = data["features"]
            log.info(f"[usgs] fetched {len(features)} earthquake events (M≥{min_magnitude}, {days_back}d)")
            return features
        except ConnectorError as e:
            log.error(f"[usgs] fetch_earthquakes failed: {e}")
            return []

    def run(self, conn: sqlite3.Connection) -> Dict:
        """Full USGS ETL — store events + compute per-state summaries."""
        start = datetime.utcnow()
        rows_in = 0
        now = self.now_iso()

        events = self.fetch_earthquakes(days_back=90)

        state_stats: Dict[str, Dict] = {}

        for event in events:
            try:
                props = event.get("properties", {})
                geo   = event.get("geometry", {})
                coords = geo.get("coordinates", [None, None, None])
                lon, lat, depth = coords[0], coords[1], coords[2]

                if lat is None or lon is None:
                    continue

                mag     = props.get("mag")
                mag_type = props.get("magType")
                place   = props.get("place", "")
                etime   = props.get("time")  # epoch ms
                event_id = event.get("id", "")

                if not event_id or mag is None:
                    continue

                # Convert epoch ms to ISO
                event_time = datetime.fromtimestamp(etime / 1000, tz=timezone.utc).isoformat()

                # Approximate state
                state = lat_lon_to_state(float(lat), float(lon))

                # Insert event
                r = conn.execute("""
                    INSERT INTO usgs_seismic
                        (event_id, magnitude, magnitude_type, depth_km, latitude, longitude,
                         state_approx, place, event_time, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(event_id) DO UPDATE SET
                        fetched_at=excluded.fetched_at
                """, (event_id, float(mag), mag_type, depth, float(lat), float(lon),
                      state, place[:200], event_time, now))
                if r.rowcount:
                    rows_in += 1

                # Accumulate per-state stats
                if state:
                    if state not in state_stats:
                        state_stats[state] = {"m25": 0, "m40": 0, "max_mag": 0.0}
                    state_stats[state]["m25"] += 1
                    if float(mag) >= 4.0:
                        state_stats[state]["m40"] += 1
                    if float(mag) > state_stats[state]["max_mag"]:
                        state_stats[state]["max_mag"] = float(mag)

            except Exception as e:
                log.warning(f"[usgs] event insert error: {e}")

        # ── Update per-state seismic summaries ──────────────────────────────
        for state, zone in STATE_SEISMIC_ZONE.items():
            stats = state_stats.get(state, {"m25": 0, "m40": 0, "max_mag": 0.0})

            # Risk multiplier: base zone (1–5) × event frequency factor
            freq_factor = 1.0 + (stats["m25"] / 50.0)          # normalize: 50 events = 2×
            high_factor = 1.0 + (stats["m40"] * 0.15)          # each M4+ adds 15%
            risk_mult = round((zone / 3.0) * freq_factor * high_factor, 3)
            risk_mult = min(risk_mult, 5.0)                      # cap at 5×

            conn.execute("""
                INSERT INTO usgs_seismic_summary
                    (state, seismic_zone, events_m25_90d, events_m40_90d, max_magnitude,
                     risk_multiplier, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(state) DO UPDATE SET
                    seismic_zone=excluded.seismic_zone,
                    events_m25_90d=excluded.events_m25_90d,
                    events_m40_90d=excluded.events_m40_90d,
                    max_magnitude=excluded.max_magnitude,
                    risk_multiplier=excluded.risk_multiplier,
                    updated_at=excluded.updated_at
            """, (state, zone, stats["m25"], stats["m40"], stats["max_mag"], risk_mult, now))

        conn.commit()
        duration = (datetime.utcnow() - start).total_seconds()
        self.log_run(conn, "success", rows_in, duration=duration)
        log.info(f"[usgs] run complete — {rows_in} events, {len(state_stats)} states, {duration:.1f}s")
        return {"source": "usgs", "rows_inserted": rows_in, "states_updated": len(STATE_SEISMIC_ZONE),
                "duration_sec": duration}
