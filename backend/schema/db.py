"""
Meridian Risk Lab — Database Schema
SQLite (dev/SaaS) — designed to be portable to PostgreSQL in production.
"""

import sqlite3
import logging
from pathlib import Path
from config import DB_PATH

log = logging.getLogger("meridian.schema")

SCHEMA_SQL = """
-- ── Meta ──────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at          TEXT NOT NULL,          -- ISO-8601
    source          TEXT NOT NULL,          -- e.g. 'eia', 'nrel', 'usgs'
    status          TEXT NOT NULL,          -- 'success' | 'partial' | 'failed'
    rows_inserted   INTEGER DEFAULT 0,
    rows_updated    INTEGER DEFAULT 0,
    error_msg       TEXT,
    duration_sec    REAL
);

-- ── EIA: Electricity Generation ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS eia_generation (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id       TEXT NOT NULL,
    fuel_type       TEXT NOT NULL,          -- 'nuclear' | 'geothermal' | 'storage'
    period          TEXT NOT NULL,          -- 'YYYY-MM'
    value_gwh       REAL,
    units           TEXT DEFAULT 'GWh',
    fetched_at      TEXT NOT NULL,
    UNIQUE(series_id, period)
);

-- ── EIA: Electricity Prices ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS eia_prices (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    state           TEXT NOT NULL,          -- 2-letter state code
    sector          TEXT NOT NULL,          -- 'commercial' | 'industrial' | 'residential'
    period          TEXT NOT NULL,          -- 'YYYY-MM'
    price_cents_kwh REAL,
    fetched_at      TEXT NOT NULL,
    UNIQUE(state, sector, period)
);

-- ── NREL: Annual Technology Baseline Cost Data ────────────────────────────────
CREATE TABLE IF NOT EXISTS nrel_atb (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    technology      TEXT NOT NULL,          -- 'Geothermal' | 'Nuclear' | 'Battery Storage'
    scenario        TEXT NOT NULL,          -- 'Moderate' | 'Advanced' | 'Conservative'
    year            INTEGER NOT NULL,
    capex_per_kw    REAL,                   -- $/kW overnight capital cost
    opex_fixed      REAL,                   -- $/kW-yr fixed O&M
    opex_var        REAL,                   -- $/MWh variable O&M
    capacity_factor REAL,                   -- decimal (0–1)
    lifetime_yrs    INTEGER,
    fetched_at      TEXT NOT NULL,
    UNIQUE(technology, scenario, year)
);

-- ── NRC: Reactor / SMR License and Event Data ─────────────────────────────────
CREATE TABLE IF NOT EXISTS nrc_reactors (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    docket_number   TEXT UNIQUE NOT NULL,
    plant_name      TEXT NOT NULL,
    reactor_type    TEXT,                   -- 'PWR' | 'BWR' | 'SMR' | 'HTGR' etc.
    state           TEXT,
    status          TEXT,                   -- 'Operating' | 'Construction' | 'License Review'
    license_issue   TEXT,                   -- date
    license_expiry  TEXT,                   -- date
    mw_capacity     REAL,
    operator        TEXT,
    fetched_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nrc_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_number    TEXT UNIQUE NOT NULL,
    plant_name      TEXT,
    state           TEXT,
    event_date      TEXT,
    event_type      TEXT,                   -- 'LER' | 'ENS' | 'Significant'
    description     TEXT,
    severity        INTEGER,                -- 1–5 scale derived
    fetched_at      TEXT NOT NULL
);

-- ── FERC: Project Filings ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ferc_projects (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    project_number  TEXT UNIQUE NOT NULL,
    project_name    TEXT,
    state           TEXT,
    technology      TEXT,
    status          TEXT,
    capacity_mw     REAL,
    filing_date     TEXT,
    applicant       TEXT,
    fetched_at      TEXT NOT NULL
);

-- ── USGS: Seismic Events ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS usgs_seismic (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT UNIQUE NOT NULL,
    magnitude       REAL NOT NULL,
    magnitude_type  TEXT,
    depth_km        REAL,
    latitude        REAL NOT NULL,
    longitude       REAL NOT NULL,
    state_approx    TEXT,
    place           TEXT,
    event_time      TEXT NOT NULL,
    fetched_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS usgs_seismic_summary (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    state           TEXT UNIQUE NOT NULL,
    seismic_zone    INTEGER,                -- 1–5
    events_m25_90d  INTEGER DEFAULT 0,      -- M≥2.5 count, last 90 days
    events_m40_90d  INTEGER DEFAULT 0,      -- M≥4.0 count, last 90 days
    max_magnitude   REAL DEFAULT 0,
    risk_multiplier REAL DEFAULT 1.0,       -- used in risk model
    updated_at      TEXT NOT NULL
);

-- ── FRED: Macro / Financial Indicators ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS fred_indicators (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id       TEXT NOT NULL,
    series_name     TEXT NOT NULL,
    period          TEXT NOT NULL,          -- 'YYYY-MM-DD'
    value           REAL,
    fetched_at      TEXT NOT NULL,
    UNIQUE(series_id, period)
);

-- ── DOE / OSTI: Technology Research Signals ──────────────────────────────────
CREATE TABLE IF NOT EXISTS doe_research (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    osti_id         TEXT UNIQUE NOT NULL,
    title           TEXT,
    technology      TEXT,
    publication_date TEXT,
    research_org    TEXT,
    sponsor_org     TEXT,
    doi             TEXT,
    fetched_at      TEXT NOT NULL
);

-- ── Derived: Risk Factor Scores (computed by risk engine) ────────────────────
CREATE TABLE IF NOT EXISTS risk_scores (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id      TEXT NOT NULL,
    computed_at     TEXT NOT NULL,
    technology_risk     REAL,
    regulatory_risk     REAL,
    construction_risk   REAL,
    counterparty_risk   REAL,
    physical_risk       REAL,
    financial_risk      REAL,
    operational_risk    REAL,
    composite_score     REAL,
    eal_usd             REAL,
    var_95_usd          REAL,
    var_99_usd          REAL,
    tvar_99_usd         REAL,
    naic_c1             REAL,
    naic_c3             REAL,
    data_version        TEXT,
    UNIQUE(project_id, computed_at)
);

-- ── Indexes ───────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_eia_gen_period    ON eia_generation(period);
CREATE INDEX IF NOT EXISTS idx_eia_price_state   ON eia_prices(state, period);
CREATE INDEX IF NOT EXISTS idx_fred_series       ON fred_indicators(series_id, period);
CREATE INDEX IF NOT EXISTS idx_usgs_time         ON usgs_seismic(event_time);
CREATE INDEX IF NOT EXISTS idx_risk_project      ON risk_scores(project_id, computed_at);
"""

def init_db(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Initialize the database, create all tables if they don't exist."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    log.info(f"Database initialized at {db_path}")
    return conn


def get_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    conn = init_db()
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    print(f"✓ Schema ready — {len(tables)} tables:")
    for t in tables:
        print(f"  · {t['name']}")
    conn.close()
