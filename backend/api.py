"""
Meridian Risk Lab — FastAPI Backend
Exposes all pipeline data and risk engine computations as REST endpoints.
Run: uvicorn api:app --reload --port 8000
"""

import sys, os, math, json, re
sys.path.insert(0, os.path.dirname(__file__))

from fastapi import FastAPI, HTTPException, UploadFile, File, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import sqlite3
import io
import csv

from schema.db import init_db, get_conn
from etl.risk_engine import RiskEngine
from etl.risk_structure import (
    FACTOR_KEYS,
    RISK_WEIGHTS,
    SUBCOMPONENT_SPECS,
    build_correlation_bundle,
    build_project_subcomponents,
)
from orchestrator import run_pipeline
from config import DB_PATH
from agent.project_import_llm import (
    AgentImportError,
    agent_import_configured,
    agent_import_info,
    file_to_payload,
    map_content_to_projects,
    verify_agent_credentials,
    OPTIONAL_FIELDS,
)

# ── Ensure output directory exists (important for Docker persistent disk mount) 
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# ── App setup ─────────────────────────────────────────────────────────────────
app = FastAPI(title="Meridian Risk Lab API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Netlify + localhost — restrict via ALLOWED_ORIGINS env var if needed
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── In-memory multi-portfolio store (replace with DB tables in production) ────
# Each portfolio holds its own independent set of projects.
_portfolios: List[dict] = [
    {
        "id": "port-flagship",
        "name": "Clean Energy Flagship",
        "description": "Diversified U.S. clean-energy book across geothermal, SMR, and storage.",
        "projects": [
            {"id": "p1", "name": "Nevada Geothermal Basin Alpha", "type": "Geothermal",
             "state": "NV", "mw": 45,   "capex": 142_000_000, "offtake": "PPA-Utility",   "status": "Operating"},
            {"id": "p2", "name": "Idaho SMR Pilot Unit 1",       "type": "Nuclear SMR",
             "state": "ID", "mw": 77,   "capex": 890_000_000, "offtake": "Merchant",       "status": "Construction"},
            {"id": "p3", "name": "Texas Grid-Scale BESS",         "type": "Battery Storage",
             "state": "TX", "mw": 200,  "capex":  78_000_000, "offtake": "Merchant",       "status": "Operating"},
            {"id": "p4", "name": "Oregon Cascade Geothermal",    "type": "Geothermal",
             "state": "OR", "mw": 30,   "capex":  98_000_000, "offtake": "PPA-Commercial", "status": "Development"},
            {"id": "p5", "name": "Wyoming SMR Cluster",          "type": "Nuclear SMR",
             "state": "WY", "mw": 154,  "capex": 1_640_000_000,"offtake": "PPA-Utility",  "status": "Development"},
        ],
    },
    {
        "id": "port-nuclear",
        "name": "Nuclear SMR Focus",
        "description": "Concentrated small modular reactor development and construction pipeline.",
        "projects": [
            {"id": "p6", "name": "Tennessee SMR Array",         "type": "Nuclear SMR",
             "state": "TN", "mw": 120,  "capex": 1_280_000_000, "offtake": "PPA-Utility",   "status": "Construction"},
            {"id": "p7", "name": "Utah Advanced SMR",           "type": "Nuclear SMR",
             "state": "UT", "mw": 92,   "capex": 1_010_000_000, "offtake": "PPA-Utility",   "status": "Development"},
            {"id": "p8", "name": "South Carolina SMR Unit 2",   "type": "Nuclear SMR",
             "state": "SC", "mw": 110,  "capex": 1_150_000_000, "offtake": "Merchant",      "status": "Development"},
            {"id": "p9", "name": "Washington SMR Demonstrator", "type": "Nuclear SMR",
             "state": "WA", "mw": 60,   "capex":  720_000_000, "offtake": "Self-Supply",    "status": "Construction"},
        ],
    },
    {
        "id": "port-storage-geo",
        "name": "Storage & Geothermal",
        "description": "Lower-risk operating storage and geothermal assets.",
        "projects": [
            {"id": "p10", "name": "Arizona Desert BESS",        "type": "Battery Storage",
             "state": "AZ", "mw": 250,  "capex":  96_000_000, "offtake": "Merchant",        "status": "Operating"},
            {"id": "p11", "name": "California Coastal Storage",  "type": "Battery Storage",
             "state": "CA", "mw": 180,  "capex":  88_000_000, "offtake": "PPA-Commercial",  "status": "Operating"},
            {"id": "p12", "name": "Utah FORGE Geothermal",      "type": "Geothermal",
             "state": "UT", "mw": 40,   "capex": 120_000_000, "offtake": "PPA-Utility",     "status": "Development"},
            {"id": "p13", "name": "Nevada Steamboat Geothermal","type": "Geothermal",
             "state": "NV", "mw": 55,   "capex": 150_000_000, "offtake": "PPA-Utility",     "status": "Operating"},
        ],
    },
]

# Active-portfolio pointer. `_projects` always references the active portfolio's
# project list object, so existing endpoints keep operating on the active book.
_active_portfolio_id: str = _portfolios[0]["id"]
_projects: List[dict] = _portfolios[0]["projects"]


def _find_portfolio(pid: str) -> Optional[dict]:
    return next((p for p in _portfolios if p["id"] == pid), None)


def _set_active_portfolio(pid: str) -> dict:
    """Rebind the active project list. Raises 404 if the portfolio is unknown."""
    global _projects, _active_portfolio_id
    port = _find_portfolio(pid)
    if not port:
        raise HTTPException(404, "Portfolio not found")
    _active_portfolio_id = pid
    _projects = port["projects"]
    return port


def _new_project_id() -> str:
    global _pid_counter
    _pid_counter += 1
    return f"p{_pid_counter}"


def _new_portfolio_id(name: str) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", (name or "portfolio").lower()).strip("-") or "portfolio"
    pid = f"port-{base}"
    existing = {p["id"] for p in _portfolios}
    if pid not in existing:
        return pid
    i = 2
    while f"{pid}-{i}" in existing:
        i += 1
    return f"{pid}-{i}"


def _resolve_target(portfolio_id: Optional[str]) -> List[dict]:
    """Target project list for a mutation: a named portfolio, or the active one."""
    if not portfolio_id:
        return _projects
    port = _find_portfolio(portfolio_id)
    if not port:
        raise HTTPException(404, "Portfolio not found")
    return port["projects"]

_pipeline_status = {"last_run": None, "running": False, "last_result": None}

# ── Pydantic models ───────────────────────────────────────────────────────────
class ProjectIn(BaseModel):
    name: str
    type: str
    state: str
    mw: float
    capex: float
    offtake: str
    status: str

class ReviewAction(BaseModel):
    item_id: int
    action: str       # "approve" | "dismiss"
    note: Optional[str] = ""

class PortfolioIn(BaseModel):
    name: str
    description: Optional[str] = ""

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_engine():
    return RiskEngine(DB_PATH)

def fmt_m(n):
    if n is None: return "$0"
    if abs(n) >= 1e9: return f"${n/1e9:.2f}B"
    return f"${n/1e6:.1f}M"


def _project_financial_assumptions(project_type: str) -> dict:
    by_type = {
        "Geothermal": {
            "capacity_factor": 0.88,
            "opex_ratio": 0.030,
            "debt_ratio": 0.55,
            "debt_rate": 0.0675,
            "debt_tenor_years": 18,
        },
        "Nuclear SMR": {
            "capacity_factor": 0.92,
            "opex_ratio": 0.025,
            "debt_ratio": 0.65,
            "debt_rate": 0.0725,
            "debt_tenor_years": 22,
        },
        "Battery Storage": {
            "capacity_factor": 0.22,
            "opex_ratio": 0.020,
            "debt_ratio": 0.50,
            "debt_rate": 0.0700,
            "debt_tenor_years": 12,
        },
    }
    return by_type.get(project_type, by_type["Geothermal"])


def _offtake_price_usd_mwh(offtake: str) -> float:
    by_offtake = {
        "PPA-Utility": 72.0,
        "PPA-Commercial": 85.0,
        "Merchant": 95.0,
        "Self-Supply": 65.0,
    }
    return by_offtake.get(offtake, 75.0)


def _variance_assumptions(project_type: str) -> dict:
    by_type = {
        "Geothermal": {
            "revenue_pct": 0.08,
            "opex_pct": 0.06,
            "ebitda_pct": 0.10,
            "debt_service_pct": 0.00,
            "net_cashflow_pct": 0.12,
            "dscr_pct": 0.08,
        },
        "Nuclear SMR": {
            "revenue_pct": 0.10,
            "opex_pct": 0.08,
            "ebitda_pct": 0.14,
            "debt_service_pct": 0.00,
            "net_cashflow_pct": 0.16,
            "dscr_pct": 0.10,
        },
        "Battery Storage": {
            "revenue_pct": 0.12,
            "opex_pct": 0.07,
            "ebitda_pct": 0.16,
            "debt_service_pct": 0.00,
            "net_cashflow_pct": 0.18,
            "dscr_pct": 0.12,
        },
    }
    return by_type.get(project_type, by_type["Geothermal"])


def _compute_irr(cashflows: list, low: float = -0.95, high: float = 1.5, steps: int = 120) -> Optional[float]:
    """
    Compute IRR from periodic cash flows with a bounded bisection solve.
    Returns decimal IRR (e.g., 0.1234 = 12.34%) or None if no real root in bounds.
    """
    if not cashflows or len(cashflows) < 2:
        return None
    if not (any(cf > 0 for cf in cashflows) and any(cf < 0 for cf in cashflows)):
        return None

    def npv(rate: float) -> float:
        return sum(cf / ((1.0 + rate) ** t) for t, cf in enumerate(cashflows))

    f_low = npv(low)
    f_high = npv(high)
    if f_low == 0:
        return low
    if f_high == 0:
        return high
    if f_low * f_high > 0:
        return None

    lo, hi = low, high
    for _ in range(steps):
        mid = (lo + hi) / 2.0
        f_mid = npv(mid)
        if abs(f_mid) < 1e-8:
            return mid
        if f_low * f_mid < 0:
            hi = mid
            f_high = f_mid
        else:
            lo = mid
            f_low = f_mid
    return (lo + hi) / 2.0


def build_financial_model(project: dict) -> dict:
    assumptions = _project_financial_assumptions(project["type"])
    variance = _variance_assumptions(project["type"])
    price_usd_mwh = _offtake_price_usd_mwh(project.get("offtake", "Merchant"))

    annual_generation_mwh = float(project["mw"]) * 8760 * assumptions["capacity_factor"]
    annual_revenue_usd = annual_generation_mwh * price_usd_mwh
    annual_opex_usd = float(project["capex"]) * assumptions["opex_ratio"]
    annual_ebitda_usd = annual_revenue_usd - annual_opex_usd

    debt_principal_usd = float(project["capex"]) * assumptions["debt_ratio"]
    equity_usd = float(project["capex"]) - debt_principal_usd
    debt_rate = assumptions["debt_rate"]
    debt_tenor = assumptions["debt_tenor_years"]
    if debt_rate > 0:
        annuity = debt_rate / (1 - (1 + debt_rate) ** (-debt_tenor))
        annual_debt_service_usd = debt_principal_usd * annuity
    else:
        annual_debt_service_usd = debt_principal_usd / debt_tenor if debt_tenor else 0.0
    dscr = (annual_ebitda_usd / annual_debt_service_usd) if annual_debt_service_usd else None

    net_operating_cashflow_usd = annual_ebitda_usd - annual_debt_service_usd
    payback_years = (
        float(project["capex"]) / annual_ebitda_usd
        if annual_ebitda_usd > 0 else None
    )

    # 10-year projection uses simple escalators to show trajectory over time.
    # This is deterministic and intended for underwriting-side scenario baselining.
    production_decay = 0.003 if project["type"] != "Battery Storage" else 0.010
    price_escalator = 0.020
    opex_escalator = 0.025
    remaining_debt = debt_principal_usd
    yearly_projection = []
    cumulative_cashflow = 0.0

    for year in range(1, 11):
        gen_y = annual_generation_mwh * ((1 - production_decay) ** (year - 1))
        price_y = price_usd_mwh * ((1 + price_escalator) ** (year - 1))
        revenue_y = gen_y * price_y
        opex_y = annual_opex_usd * ((1 + opex_escalator) ** (year - 1))
        ebitda_y = revenue_y - opex_y

        if year <= debt_tenor and remaining_debt > 0:
            interest_y = remaining_debt * debt_rate
            principal_y = max(annual_debt_service_usd - interest_y, 0.0)
            principal_y = min(principal_y, remaining_debt)
            debt_service_y = interest_y + principal_y
            remaining_debt = max(remaining_debt - principal_y, 0.0)
        else:
            interest_y = 0.0
            principal_y = 0.0
            debt_service_y = 0.0

        net_cashflow_y = ebitda_y - debt_service_y
        cumulative_cashflow += net_cashflow_y
        dscr_y = (ebitda_y / debt_service_y) if debt_service_y > 0 else None

        yearly_projection.append({
            "year": year,
            "generation_mwh": round(gen_y, 2),
            "price_usd_mwh": round(price_y, 2),
            "revenue_usd": round(revenue_y, 2),
            "opex_usd": round(opex_y, 2),
            "ebitda_usd": round(ebitda_y, 2),
            "interest_usd": round(interest_y, 2),
            "principal_usd": round(principal_y, 2),
            "debt_service_usd": round(debt_service_y, 2),
            "net_cashflow_usd": round(net_cashflow_y, 2),
            "cumulative_cashflow_usd": round(cumulative_cashflow, 2),
            "ending_debt_usd": round(remaining_debt, 2),
            "dscr": round(dscr_y, 3) if dscr_y is not None else None,
            "variance": {
                "revenue_usd": round(abs(revenue_y) * variance["revenue_pct"], 2),
                "opex_usd": round(abs(opex_y) * variance["opex_pct"], 2),
                "ebitda_usd": round(abs(ebitda_y) * variance["ebitda_pct"], 2),
                "interest_usd": round(abs(interest_y) * variance["debt_service_pct"], 2),
                "principal_usd": round(abs(principal_y) * variance["debt_service_pct"], 2),
                "debt_service_usd": round(abs(debt_service_y) * variance["debt_service_pct"], 2),
                "net_cashflow_usd": round(abs(net_cashflow_y) * variance["net_cashflow_pct"], 2),
                "cumulative_cashflow_usd": round(abs(cumulative_cashflow) * variance["net_cashflow_pct"], 2),
                "ending_debt_usd": round(abs(remaining_debt) * variance["debt_service_pct"], 2),
                "dscr": round((abs(dscr_y) * variance["dscr_pct"]), 3) if dscr_y is not None else None,
            },
        })

    # Equity IRR: initial equity investment at t0, followed by 10 years of net cash flows.
    equity_cashflows = [-equity_usd] + [float(y["net_cashflow_usd"]) for y in yearly_projection]
    irr = _compute_irr(equity_cashflows)

    return {
        "assumptions": {
            "capacity_factor": assumptions["capacity_factor"],
            "price_usd_mwh": price_usd_mwh,
            "opex_ratio": assumptions["opex_ratio"],
            "debt_ratio": assumptions["debt_ratio"],
            "debt_rate": assumptions["debt_rate"],
            "debt_tenor_years": assumptions["debt_tenor_years"],
            "variance": variance,
        },
        "metrics": {
            "annual_generation_mwh": round(annual_generation_mwh, 2),
            "annual_revenue_usd": round(annual_revenue_usd, 2),
            "annual_opex_usd": round(annual_opex_usd, 2),
            "annual_ebitda_usd": round(annual_ebitda_usd, 2),
            "debt_principal_usd": round(debt_principal_usd, 2),
            "equity_usd": round(equity_usd, 2),
            "annual_debt_service_usd": round(annual_debt_service_usd, 2),
            "net_operating_cashflow_usd": round(net_operating_cashflow_usd, 2),
            "dscr": round(dscr, 3) if dscr is not None else None,
            "payback_years": round(payback_years, 2) if payback_years is not None else None,
            "irr_decimal": round(irr, 6) if irr is not None else None,
            "irr_pct": round(irr * 100, 2) if irr is not None else None,
        },
        "yearly_projection": yearly_projection,
    }


def attach_financial_model(project: dict) -> dict:
    project["financial_model"] = build_financial_model(project)
    return project


def attach_project_subcomponents(project: dict) -> dict:
    project["correlation_subcomponents"] = build_project_subcomponents(project)
    return project


def attach_project_analytics(project: dict) -> dict:
    attach_project_subcomponents(project)
    attach_financial_model(project)
    return project


_pid_counter = 0
for _port in _portfolios:
    for _p in _port["projects"]:
        attach_project_analytics(_p)
        _pid_counter += 1


def _ingest_project_dicts(rows: List[dict], row_base: int = 1,
                          target: Optional[List[dict]] = None) -> dict:
    """Append validated projects to a target list (active portfolio by default)."""
    target = _projects if target is None else target
    added: List[dict] = []
    errors: List[dict] = []
    for i, row in enumerate(rows):
        try:
            if not isinstance(row, dict):
                raise ValueError("row must be an object")
            p = {
                "id":      _new_project_id(),
                "name":    str(row["name"]).strip(),
                "type":    str(row["type"]).strip(),
                "state":   str(row["state"]).strip().upper(),
                "mw":      float(row["mw"]),
                "capex":   float(row["capex"]),
                "offtake": str(row["offtake"]).strip(),
                "status":  str(row["status"]).strip(),
            }
            # Optional enrichment fields (developer, cod, notes) when present.
            for opt in OPTIONAL_FIELDS:
                val = row.get(opt)
                if val not in (None, ""):
                    p[opt] = str(val).strip()
            p = attach_project_analytics(p)
            added.append(p)
        except Exception as e:
            errors.append({"row": row_base + i, "error": str(e)})
    target.extend(added)
    return {"added": len(added), "errors": errors, "projects": added}


# ══════════════════════════════════════════════════════════════════════════════
# HEALTH / META
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/health")
def health():
    conn = init_db(DB_PATH)
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    counts = {}
    for t in tables:
        counts[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    conn.close()
    return {"status": "ok", "db_tables": counts, "timestamp": datetime.utcnow().isoformat()}


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE CONTROL
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/api/pipeline/run")
def trigger_pipeline(background_tasks: BackgroundTasks, sources: Optional[List[str]] = None):
    """Trigger a pipeline run (async background)."""
    if _pipeline_status["running"]:
        return {"status": "already_running"}

    def _run():
        _pipeline_status["running"] = True
        try:
            result = run_pipeline(sources=sources)
            _pipeline_status["last_result"] = result
            _pipeline_status["last_run"] = datetime.utcnow().isoformat()
        finally:
            _pipeline_status["running"] = False

    background_tasks.add_task(_run)
    return {"status": "started", "sources": sources or "all"}


@app.get("/api/pipeline/status")
def pipeline_status():
    conn = get_conn(DB_PATH)
    recent_runs = conn.execute("""
        SELECT source, status, run_at, rows_inserted, duration_sec, error_msg
        FROM pipeline_runs ORDER BY run_at DESC LIMIT 20
    """).fetchall()
    conn.close()
    return {
        "running":   _pipeline_status["running"],
        "last_run":  _pipeline_status["last_run"],
        "recent_runs": [dict(r) for r in recent_runs],
    }


# ══════════════════════════════════════════════════════════════════════════════
# PORTFOLIOS
# ══════════════════════════════════════════════════════════════════════════════

def _portfolio_summary(port: dict) -> dict:
    """Lightweight summary for the home screen (no Monte Carlo)."""
    projs = port["projects"]
    total_capex = sum(float(p.get("capex", 0)) for p in projs)
    total_mw = sum(float(p.get("mw", 0)) for p in projs)
    types: dict = {}
    for p in projs:
        types[p["type"]] = types.get(p["type"], 0) + 1

    avg_composite = None
    if projs:
        try:
            engine = get_engine()
            comps = [
                engine.score_project(
                    technology=p["type"], state=p["state"], capex=p["capex"],
                    mw=p["mw"], status=p["status"], offtake=p["offtake"],
                )["composite"]
                for p in projs
            ]
            avg_composite = round(sum(comps) / len(comps), 1)
        except Exception as e:
            log.warning(f"[portfolio] summary scoring failed: {e}")

    return {
        "id": port["id"],
        "name": port["name"],
        "description": port.get("description", ""),
        "project_count": len(projs),
        "total_capex": total_capex,
        "total_mw": total_mw,
        "types": types,
        "avg_composite": avg_composite,
        "active": port["id"] == _active_portfolio_id,
    }


@app.get("/api/portfolios")
def list_portfolios():
    return {
        "portfolios": [_portfolio_summary(p) for p in _portfolios],
        "active_id": _active_portfolio_id,
        "count": len(_portfolios),
    }


@app.post("/api/portfolios")
def create_portfolio(body: PortfolioIn):
    name = (body.name or "").strip()
    if not name:
        raise HTTPException(400, "Portfolio name is required.")
    pid = _new_portfolio_id(name)
    port = {
        "id": pid,
        "name": name,
        "description": (body.description or "").strip(),
        "projects": [],
    }
    _portfolios.append(port)
    return {
        "status": "created",
        "portfolio": _portfolio_summary(port),
        "active_id": _active_portfolio_id,
    }


@app.post("/api/portfolios/{pid}/activate")
def activate_portfolio(pid: str):
    _set_active_portfolio(pid)
    return {"status": "activated", "active_id": _active_portfolio_id}


@app.delete("/api/portfolios/{pid}")
def delete_portfolio(pid: str):
    global _projects, _active_portfolio_id
    if len(_portfolios) <= 1:
        raise HTTPException(400, "Cannot delete the only portfolio.")
    port = _find_portfolio(pid)
    if not port:
        raise HTTPException(404, "Portfolio not found")
    _portfolios[:] = [p for p in _portfolios if p["id"] != pid]
    if _active_portfolio_id == pid:
        _active_portfolio_id = _portfolios[0]["id"]
        _projects = _portfolios[0]["projects"]
    return {"status": "deleted", "active_id": _active_portfolio_id}


# ══════════════════════════════════════════════════════════════════════════════
# PROJECTS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/projects")
def list_projects():
    return {
        "projects": _projects,
        "count": len(_projects),
        "portfolio_id": _active_portfolio_id,
    }


@app.post("/api/projects")
def add_project(project: ProjectIn, portfolio_id: Optional[str] = None):
    target = _resolve_target(portfolio_id)
    p = attach_project_analytics({"id": _new_project_id(), **project.dict()})
    target.append(p)
    return {"status": "created", "project": p}


@app.delete("/api/projects/{project_id}")
def delete_project(project_id: str):
    # Mutate the active list in place so the portfolio store stays in sync.
    before = len(_projects)
    _projects[:] = [p for p in _projects if p["id"] != project_id]
    if len(_projects) == before:
        raise HTTPException(404, "Project not found")
    return {"status": "deleted"}


@app.get("/api/projects/{project_id}/financial-model")
def get_project_financial_model(project_id: str):
    p = next((x for x in _projects if x["id"] == project_id), None)
    if not p:
        raise HTTPException(404, "Project not found")
    return {
        "project_id": p["id"],
        "project_name": p["name"],
        "financial_model": p.get("financial_model") or build_financial_model(p),
    }


@app.get("/api/financial-models")
def list_financial_models():
    models = []
    for p in _projects:
        fm = p.get("financial_model") or build_financial_model(p)
        models.append({
            "project_id": p["id"],
            "project_name": p["name"],
            "project_type": p["type"],
            "state": p["state"],
            "financial_model": fm,
        })
    return {"models": models, "count": len(models)}


@app.get("/api/projects/import-agent-status")
def import_agent_status():
    """Whether AI-assisted import is available (requires ANTHROPIC_API_KEY)."""
    return agent_import_info()


@app.get("/api/projects/import-agent-check")
def import_agent_check():
    """Lightweight live probe of the Claude credentials/model."""
    return verify_agent_credentials()


@app.post("/api/projects/upload")
async def upload_projects(file: UploadFile = File(...), portfolio_id: Optional[str] = None):
    """Accept CSV with columns: name, type, state, mw, capex, offtake, status."""
    target = _resolve_target(portfolio_id)
    content = await file.read()
    try:
        text = content.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
    except Exception as e:
        raise HTTPException(400, f"Could not parse file: {e}")
    return _ingest_project_dicts(rows, row_base=2, target=target)


@app.post("/api/projects/upload-agent")
async def upload_projects_agent(file: UploadFile = File(...), portfolio_id: Optional[str] = None):
    """Map a CSV/Excel register OR a PDF/text document to project rows via Claude,
    then validate and merge into the target portfolio."""
    target = _resolve_target(portfolio_id)
    fn = (file.filename or "").lower()
    allowed = (".csv", ".xlsx", ".xls", ".pdf", ".txt", ".md")
    if not any(fn.endswith(ext) for ext in allowed):
        raise HTTPException(400, "Use a .csv, .xlsx, .xls, .pdf, .txt, or .md file.")

    content = await file.read()
    if not content:
        raise HTTPException(400, "Empty file.")

    try:
        text, kind = file_to_payload(content, file.filename or "upload")
        raw_rows = map_content_to_projects(text, kind)
    except AgentImportError as e:
        raise HTTPException(e.status_code, e.message)

    out = _ingest_project_dicts(raw_rows, row_base=1, target=target)
    out["agent"] = True
    out["source_kind"] = kind
    return out


# ══════════════════════════════════════════════════════════════════════════════
# RISK ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/risk/project/{project_id}")
def analyze_project(project_id: str):
    p = next((x for x in _projects if x["id"] == project_id), None)
    if not p:
        raise HTTPException(404, "Project not found")
    engine = get_engine()
    result = engine.analyze_project(p)
    return {**result, "project": p}


@app.get("/api/risk/portfolio")
def analyze_portfolio():
    if not _projects:
        return {"error": "No projects in portfolio"}
    engine = get_engine()
    result = engine.analyze_portfolio(_projects)

    # Enrich each project result with its name
    for i, pr in enumerate(result["projects"]):
        pr["project_name"] = _projects[i]["name"]
        pr["project_type"] = _projects[i]["type"]
        pr["project_state"] = _projects[i]["state"]
        pr["project_capex"] = _projects[i]["capex"]

    return result


@app.get("/api/risk/correlation")
def get_correlation():
    sub = build_correlation_bundle()
    return {
        "labels": sub["labels"],
        "keys": sub["keys"],
        "matrix": sub["matrix"],
        "weights": sub["weights"],
        "parent_factors": sub["parents"],
        "descriptions": sub["descriptions"],
        "grouped_matrices": sub["grouped_matrices"],
        "category_labels": sub["category_labels"],
        "category_matrix": sub["category_matrix"],
        "category_weights": sub["category_weights"],
        "rollup_method": (
            "Category scores = mean of 8 subcomponent scores per parent. "
            "Category correlations = covariance rollup from the 64×64 subcomponent matrix "
            "(used in Monte Carlo)."
        ),
    }


@app.get("/api/risk/correlation/between-projects")
def get_between_projects_correlation():
    """
    N×N matrix of implied correlations between portfolio projects.
    Built from aligned subcomponent score profiles (centered cosine / correlation),
    with bonuses for same technology type and same state (shared exposure channels).
    """
    if not _projects:
        return {
            "project_ids": [],
            "labels": [],
            "full_names": [],
            "matrix": [],
            "count": 0,
            "method": "",
        }

    score_vecs = []
    for p in _projects:
        subs = p.get("correlation_subcomponents") or build_project_subcomponents(p)
        labels = [x[0] for x in SUBCOMPONENT_SPECS]
        score_vecs.append([float(subs[l]["score"]) for l in labels])

    n = len(_projects)

    def pair_corr(i: int, j: int) -> float:
        if i == j:
            return 1.0
        vi, vj = score_vecs[i], score_vecs[j]
        mi = sum(vi) / len(vi)
        mj = sum(vj) / len(vj)
        ci = [a - mi for a in vi]
        cj = [b - mj for b in vj]
        dot = sum(a * b for a, b in zip(ci, cj))
        ni = math.sqrt(sum(a * a for a in ci)) or 1e-9
        nj = math.sqrt(sum(b * b for b in cj)) or 1e-9
        r = dot / (ni * nj)
        r = max(-1.0, min(1.0, r))
        base = 0.40 + 0.50 * r
        pi, pj = _projects[i], _projects[j]
        if pi["type"] == pj["type"]:
            base += 0.08
        if pi["state"] == pj["state"]:
            base += 0.14
        return round(max(0.05, min(0.99, base)), 3)

    matrix = [[pair_corr(i, j) for j in range(n)] for i in range(n)]

    def short_label(name: str) -> str:
        parts = (name or "").split()
        s = " ".join(parts[:3]) if parts else "?"
        return s if len(s) <= 28 else s[:25] + "…"

    return {
        "project_ids": [p["id"] for p in _projects],
        "labels": [short_label(p["name"]) for p in _projects],
        "full_names": [p["name"] for p in _projects],
        "matrix": matrix,
        "count": n,
        "method": (
            f"Subcomponent-profile similarity: centered correlation of {len(SUBCOMPONENT_SPECS)} project "
            "subcomponent scores, plus bonuses for shared technology type and state."
        ),
    }


# ══════════════════════════════════════════════════════════════════════════════
# DATA FEEDS (pipeline DB read-outs)
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/data/nrel-atb")
def get_nrel_atb(technology: Optional[str] = None, scenario: str = "Moderate"):
    conn = get_conn(DB_PATH)
    q = "SELECT * FROM nrel_atb WHERE scenario=?"
    params = [scenario]
    if technology:
        q += " AND technology=?"
        params.append(technology)
    q += " ORDER BY technology, year"
    rows = [dict(r) for r in conn.execute(q, params).fetchall()]
    conn.close()
    return {"rows": rows, "count": len(rows), "scenario": scenario}


@app.get("/api/data/seismic")
def get_seismic(state: Optional[str] = None):
    conn = get_conn(DB_PATH)
    if state:
        rows = conn.execute(
            "SELECT * FROM usgs_seismic_summary WHERE state=?", (state,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM usgs_seismic_summary ORDER BY risk_multiplier DESC"
        ).fetchall()
    conn.close()
    return {"rows": [dict(r) for r in rows]}


@app.get("/api/data/nrc-reactors")
def get_nrc_reactors(reactor_type: Optional[str] = None):
    conn = get_conn(DB_PATH)
    if reactor_type:
        rows = conn.execute(
            "SELECT * FROM nrc_reactors WHERE reactor_type=? ORDER BY status",
            (reactor_type,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM nrc_reactors ORDER BY reactor_type, status"
        ).fetchall()
    conn.close()
    return {"rows": [dict(r) for r in rows]}


@app.get("/api/data/nrc-events")
def get_nrc_events():
    conn = get_conn(DB_PATH)
    rows = conn.execute(
        "SELECT * FROM nrc_events ORDER BY event_date DESC"
    ).fetchall()
    conn.close()
    return {"rows": [dict(r) for r in rows]}


@app.get("/api/data/fred")
def get_fred_indicators():
    conn = get_conn(DB_PATH)
    # Get latest value per series
    rows = conn.execute("""
        SELECT series_id, series_name, value, period
        FROM fred_indicators
        WHERE (series_id, period) IN (
            SELECT series_id, MAX(period) FROM fred_indicators GROUP BY series_id
        )
        ORDER BY series_id
    """).fetchall()
    conn.close()
    return {"indicators": [dict(r) for r in rows]}


@app.get("/api/data/eia-generation")
def get_eia_generation():
    conn = get_conn(DB_PATH)
    rows = conn.execute("""
        SELECT fuel_type, period, value_gwh
        FROM eia_generation
        ORDER BY fuel_type, period DESC
    """).fetchall()
    conn.close()
    return {"rows": [dict(r) for r in rows]}


@app.get("/api/data/eia-prices")
def get_eia_prices(state: Optional[str] = None):
    conn = get_conn(DB_PATH)
    if state:
        rows = conn.execute(
            "SELECT * FROM eia_prices WHERE state=? ORDER BY period DESC LIMIT 12",
            (state,)
        ).fetchall()
    else:
        rows = conn.execute("""
            SELECT state, sector, period, price_cents_kwh FROM eia_prices
            WHERE (state, period) IN (
                SELECT state, MAX(period) FROM eia_prices GROUP BY state
            )
            ORDER BY state
        """).fetchall()
    conn.close()
    return {"rows": [dict(r) for r in rows]}


# ══════════════════════════════════════════════════════════════════════════════
# NAIC RBC SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/naic/summary")
def naic_summary():
    engine = get_engine()
    portfolio = engine.analyze_portfolio(_projects)

    project_rows = []
    for i, pr in enumerate(portfolio["projects"]):
        p = _projects[i]
        rbc = pr["naic_rbc"]
        losses = pr["losses"]
        scores = pr["scores"]
        ratio = 2.1 + (50 - scores["composite"]) / 50 * 1.5
        project_rows.append({
            "id":           p["id"],
            "name":         p["name"],
            "type":         p["type"],
            "state":        p["state"],
            "composite":    round(scores["composite"], 1),
            "eal":          losses["eal"],
            "var_99":       losses["var_99"],
            "rbc_c1":       rbc["c1"],
            "rbc_c2":       rbc["c2"],
            "rbc_c3":       rbc["c3"],
            "rbc_c4":       rbc["c4"],
            "rbc_total":    rbc["total"],
            "rbc_ratio":    round(ratio, 2),
            "action_level": "No Action" if ratio > 2.5 else
                            "Company Action" if ratio > 2.0 else "Regulatory Action",
        })

    return {
        "projects":             project_rows,
        "portfolio_eal":        portfolio["total_eal"],
        "portfolio_var99_net":  portfolio["total_var99_net"],
        "portfolio_rbc_net":    portfolio["total_rbc_net"],
        "diversification_pct":  portfolio["diversification_pct"],
    }


# ══════════════════════════════════════════════════════════════════════════════
# HUMAN REVIEW
# ══════════════════════════════════════════════════════════════════════════════

_review_actions: dict = {}   # item_id -> {action, note, timestamp}

@app.get("/api/review/items")
def get_review_items():
    """Generate review flags from live risk data."""
    engine = get_engine()
    items = []

    for p in _projects:
        result = engine.analyze_project(p)
        scores = result["scores"]
        rbc = result["naic_rbc"]
        losses = result["losses"]

        if scores["composite"] >= 85:
            items.append({
                "id": len(items) + 1,
                "type": "Risk Score",
                "project": p["name"],
                "flag": f"Composite score {scores['composite']:.0f} exceeds threshold of 85",
                "severity": "high",
                "data": {"score": scores["composite"]}
            })

        if p["type"] == "Nuclear SMR" and scores["regulatory"] >= 75:
            items.append({
                "id": len(items) + 1,
                "type": "Regulatory",
                "project": p["name"],
                "flag": f"NRC regulatory risk score elevated at {scores['regulatory']:.0f}",
                "severity": "high",
                "data": {"regulatory_score": scores["regulatory"]}
            })

        capex_ratio = p["capex"] / (p["mw"] * 1000 * 8850) if p["type"] == "Nuclear SMR" else \
                      p["capex"] / (p["mw"] * 1000 * 5600) if p["type"] == "Geothermal" else \
                      p["capex"] / (p["mw"] * 1000 * 1220)
        if capex_ratio > 1.3:
            items.append({
                "id": len(items) + 1,
                "type": "CapEx vs Benchmark",
                "project": p["name"],
                "flag": f"CapEx is {(capex_ratio-1)*100:.0f}% above NREL ATB benchmark",
                "severity": "medium",
                "data": {"ratio": round(capex_ratio, 2)}
            })

    # Portfolio-level flag
    conn = get_conn(DB_PATH)
    smr_events = conn.execute(
        "SELECT COUNT(*) FROM nrc_events WHERE severity >= 3 AND event_date >= date('now','-90 days')"
    ).fetchone()[0]
    conn.close()

    if smr_events > 0:
        items.append({
            "id": len(items) + 1,
            "type": "NRC Event",
            "project": "SMR Projects",
            "flag": f"{smr_events} high-severity NRC events in last 90 days — review required",
            "severity": "high",
            "data": {"event_count": smr_events}
        })

    items.append({
        "id": len(items) + 1,
        "type": "Reserve Memo",
        "project": "All Projects",
        "flag": "NAIC RBC C-1 reserve calculation ready for actuarial sign-off",
        "severity": "low",
        "data": {}
    })

    # Merge in any existing review actions
    for item in items:
        if item["id"] in _review_actions:
            item["review"] = _review_actions[item["id"]]

    pending = sum(1 for i in items if i["id"] not in _review_actions)
    return {"items": items, "total": len(items), "pending": pending}


@app.post("/api/review/action")
def submit_review_action(action: ReviewAction):
    _review_actions[action.item_id] = {
        "action":    action.action,
        "note":      action.note,
        "timestamp": datetime.utcnow().isoformat(),
    }
    return {"status": "recorded", "item_id": action.item_id}


@app.get("/api/review/locked")
def review_locked():
    """Returns True if outputs should be locked (pending reviews exist)."""
    items_resp = get_review_items()
    return {"locked": items_resp["pending"] > 0, "pending": items_resp["pending"]}


if __name__ == "__main__":
    import uvicorn
    init_db(DB_PATH)
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
