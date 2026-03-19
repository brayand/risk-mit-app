"""
Meridian Risk Lab — Pipeline Configuration
All public API endpoints, rate limits, and shared constants.
"""

# ── API Endpoints (all public / no key required) ──────────────────────────────

EIA_BASE          = "https://api.eia.gov/v2"
EIA_API_KEY       = _os.getenv("EIA_API_KEY", "DEMO_KEY")     # Free: eia.gov/opendata

NREL_BASE         = "https://developer.nrel.gov/api"
NREL_API_KEY      = _os.getenv("NREL_API_KEY", "DEMO_KEY")    # Free: developer.nrel.gov

NRC_ADAMS_BASE    = "https://adams.nrc.gov/wba/services/search/results"
NRC_EVENTS_BASE   = "https://www.nrc.gov/reading-rm/doc-collections/event-status"

FERC_ELIBRARY     = "https://elibrary.ferc.gov/eLibrary/search"
FERC_RSS_BASE     = "https://www.ferc.gov/rss"

USGS_EARTHQUAKE   = "https://earthquake.usgs.gov/fdsnws/event/1/query"
USGS_GEOTHERMAL   = "https://mrdata.usgs.gov/services/wfs/hot-springs"

DOE_OSTI          = "https://www.osti.gov/api/v1/records"
DOE_ARPA_E        = "https://arpa-e.energy.gov/api/projects"  # HTML scrape fallback

FRED_BASE         = "https://api.stlouisfed.org/fred/series/observations"
FRED_API_KEY      = _os.getenv("FRED_API_KEY", "")            # Free: fred.stlouisfed.org

# ── FRED Series IDs relevant to clean energy risk ─────────────────────────────
FRED_SERIES = {
    "fed_funds_rate":        "FEDFUNDS",
    "ten_year_treasury":     "DGS10",
    "industrial_prod_index": "INDPRO",
    "ppi_energy":            "PPIFES",
    "construction_cost_idx": "WPUSI012011",
    "capacity_utilization":  "TCU",
}

# ── NREL Annual Technology Baseline (ATB) relevant rows ───────────────────────
NREL_ATB_TECHS = [
    "Geothermal",
    "Nuclear",          # covers SMR proxy data
    "Utility-Scale Battery Storage",
]

# ── EIA Series: electricity generation by fuel type (monthly) ─────────────────
EIA_ELEC_SERIES = {
    "nuclear_gen_gwh":      "EBA.US48-ALL.NG.NUC.H",
    "geothermal_gen_gwh":   "EBA.US48-ALL.NG.GEO.H",
    "battery_storage_gwh":  "EBA.US48-ALL.NG.SUN.H",  # proxy via storage dispatch
}

# ── USGS Seismic parameters (U.S., M≥2.5, last 30 days on demand) ─────────────
USGS_EQ_PARAMS = {
    "format":    "geojson",
    "minmagnitude": 2.5,
    "maxlatitude":  49.5,
    "minlatitude":  24.5,
    "maxlongitude": -66.9,
    "minlongitude": -125.0,
}

# ── State → USGS seismic hazard zone mapping (NSHM 2023) ─────────────────────
STATE_SEISMIC_ZONE = {
    "AK": 5, "CA": 5, "WA": 4, "OR": 4, "NV": 4, "UT": 3,
    "ID": 3, "MT": 3, "WY": 3, "CO": 3, "AZ": 2, "NM": 2,
    "TX": 1, "OK": 2, "KS": 1, "NE": 1, "SD": 1, "ND": 1,
    "MN": 1, "IA": 1, "MO": 2, "AR": 3, "TN": 2, "SC": 2,
    "NC": 1, "VA": 1, "WV": 1, "KY": 1, "OH": 1, "IN": 1,
    "IL": 2, "MI": 1, "WI": 1, "GA": 1, "FL": 1, "AL": 1,
    "MS": 1, "LA": 1, "PA": 1, "NY": 1, "NJ": 1, "CT": 1,
    "RI": 1, "MA": 1, "VT": 1, "NH": 1, "ME": 1, "MD": 1,
    "DE": 1, "HI": 4,
}

# ── Pipeline settings ─────────────────────────────────────────────────────────
import os as _os
DB_PATH           = _os.getenv("DB_PATH", _os.path.join(_os.path.dirname(__file__), "output", "meridian.db"))
LOG_PATH          = "/home/claude/pipeline/output/pipeline.log"
CACHE_TTL_DAYS    = 30       # refresh data if older than this
REQUEST_TIMEOUT   = 20       # seconds per HTTP request
MAX_RETRIES       = 3
RETRY_BACKOFF     = 2.0      # exponential backoff base (seconds)
