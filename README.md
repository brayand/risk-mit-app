# Meridian Risk Lab
### Clean Energy Technology Innovation Risk Modeling Platform

U.S.-focused SaaS for modeling, quantifying, and reporting risk across **Battery Storage**, **Nuclear SMR**, and **Geothermal** projects — built for insurers, project developers, banks, and regulators.

---

## Project Structure

```
meridian-risk-lab/
├── backend/                  # Python FastAPI backend + data pipeline
│   ├── api.py                # FastAPI app — 12 REST endpoints
│   ├── config.py             # API keys, endpoints, DB path, constants
│   ├── orchestrator.py       # Pipeline runner (all sources or selective)
│   ├── requirements.txt      # Python dependencies
│   ├── connectors/
│   │   ├── base.py           # HTTP client with retry/backoff
│   │   ├── eia.py            # EIA electricity generation + state prices
│   │   ├── nrel.py           # NREL ATB 2024 cost benchmarks
│   │   ├── usgs.py           # USGS seismic events + state risk summaries
│   │   └── fred_nrc.py       # FRED macro indicators + NRC reactor/event data
│   ├── schema/
│   │   └── db.py             # SQLite schema (11 tables) + init/connect helpers
│   ├── etl/
│   │   └── risk_engine.py    # Risk scoring, Monte Carlo, NAIC RBC calculator
│   ├── tests/
│   │   └── test_pipeline.py  # Integration test suite (33/35 passing)
│   └── output/
│       └── meridian.db       # SQLite database (pre-seeded with NREL + NRC data)
│
└── frontend/                 # React UI + Vite dev server (single entry file)
    ├── index.html            # Single frontend app source
    ├── package.json
    └── vite.config.js        # Dev proxy: /api → localhost:8000
```

---

## Quick Start

### 1. Backend

```bash
cd backend
pip install -r requirements.txt

# Start the API server
uvicorn api:app --reload --port 8000
```

The API will be live at `http://localhost:8000`.  
Interactive docs at `http://localhost:8000/docs`.

### 2. Frontend

```bash
cd frontend
npm install
npm run dev
# → http://localhost:3000
```

The UI lives in `index.html` (React via CDN + in-browser Babel). The Vite dev proxy forwards `/api/*` to the backend automatically.

### 3. Run the data pipeline

```bash
cd backend

# Full pipeline (all 5 sources)
python orchestrator.py

# Single source
python orchestrator.py --source usgs

# Dry run (no DB writes)
python orchestrator.py --test
```

Or trigger from the UI via the **↻ SYNC DATA** button in the sidebar.

---

## API Keys Required (all free)

Add these to `backend/config.py`:

| Source | Key Location | Config Variable |
|--------|-------------|-----------------|
| EIA    | [eia.gov/opendata](https://www.eia.gov/opendata/) | `EIA_API_KEY` |
| FRED   | [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html) | `FRED_API_KEY` |
| NREL   | [developer.nrel.gov](https://developer.nrel.gov/signup/) | `NREL_API_KEY` |

USGS and NRC require no API keys.

---

## Data Sources

| Source | Data | Update |
|--------|------|--------|
| **NREL ATB 2024** | CapEx, OpEx, capacity factors for Geothermal, Nuclear, Storage | Annual (embedded) |
| **EIA API v2** | Monthly electricity generation by fuel type; retail prices by state | Monthly |
| **USGS FDSN** | M≥2.5 earthquake events, per-state seismic risk multipliers | On-demand |
| **FRED API** | Fed funds rate, 10Y Treasury, PPI, construction cost index | Monthly |
| **NRC ADAMS** | Reactor license status, SMR applications, event logs | Monthly |

---

## Risk Model

**8 weighted risk factors** (weights sum to 1.0):

| Factor | Weight | Driven by |
|--------|--------|-----------|
| Technology | 22% | NREL ATB benchmarks, CapEx vs. benchmark ratio |
| Regulatory | 18% | NRC license stage, recent event severity |
| Construction | 15% | CapEx ratio, FRED construction cost index |
| Counterparty | 12% | Offtake structure (PPA-Utility → Merchant) |
| Physical | 11% | USGS seismic zone + recent event frequency |
| Financial | 10% | FRED 10Y Treasury rate, project size |
| Operational | 8% | Technology-specific base rates |
| Workforce | 4% | Technology-specific skills gap estimates |

**Loss distribution**: Monte Carlo (10,000 trials), Cholesky-decomposed t-Copula, Poisson frequency × Log-normal severity.

**NAIC RBC**: C-1 (asset risk), C-2 (insurance risk), C-3 (interest rate), C-4 (business risk) — covariance formula per NAIC standard.

---

## Regulatory Frameworks (user-selectable)

- **NAIC RBC** ← primary (U.S. insurance)
- Solvency II
- Lloyd's RDS
- Basel IV
- TCFD
- IFRS 9

---

## Roadmap

- [ ] PDF export (ReportLab + Jinja2 templates)
- [ ] Excel export (openpyxl — risk register + RBC tables)
- [ ] Actuarial reserve memo generator
- [ ] User authentication (JWT)
- [ ] Multi-user org accounts
- [ ] PostgreSQL migration for production
- [ ] Scheduled monthly pipeline runs (APScheduler)
- [ ] FERC project filing connector
