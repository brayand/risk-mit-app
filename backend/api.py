"""
Meridian Risk Lab — FastAPI Backend
Exposes all pipeline data and risk engine computations as REST endpoints.
Run: uvicorn api:app --reload --port 8000
"""

import sys, os, math, json
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
from etl.risk_engine import RiskEngine, FACTOR_KEYS, CORR, RISK_WEIGHTS
from orchestrator import run_pipeline
from config import DB_PATH

# ── App setup ─────────────────────────────────────────────────────────────────
app = FastAPI(title="Meridian Risk Lab API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Netlify + localhost — restrict via ALLOWED_ORIGINS env var if needed
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── In-memory project store (replace with DB table in production) ─────────────
_projects: List[dict] = [
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
]

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

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_engine():
    return RiskEngine(DB_PATH)

def fmt_m(n):
    if n is None: return "$0"
    if abs(n) >= 1e9: return f"${n/1e9:.2f}B"
    return f"${n/1e6:.1f}M"

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
# PROJECTS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/projects")
def list_projects():
    return {"projects": _projects, "count": len(_projects)}


@app.post("/api/projects")
def add_project(project: ProjectIn):
    new_id = f"p{len(_projects) + 1}"
    p = {"id": new_id, **project.dict()}
    _projects.append(p)
    return {"status": "created", "project": p}


@app.delete("/api/projects/{project_id}")
def delete_project(project_id: str):
    global _projects
    before = len(_projects)
    _projects = [p for p in _projects if p["id"] != project_id]
    if len(_projects) == before:
        raise HTTPException(404, "Project not found")
    return {"status": "deleted"}


@app.post("/api/projects/upload")
async def upload_projects(file: UploadFile = File(...)):
    """Accept CSV or simple XLSX upload of multiple projects."""
    content = await file.read()
    added = []
    errors = []

    try:
        text = content.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        for i, row in enumerate(reader):
            try:
                p = {
                    "id":      f"p{len(_projects) + len(added) + 1}",
                    "name":    row["name"].strip(),
                    "type":    row["type"].strip(),
                    "state":   row["state"].strip().upper(),
                    "mw":      float(row["mw"]),
                    "capex":   float(row["capex"]),
                    "offtake": row["offtake"].strip(),
                    "status":  row["status"].strip(),
                }
                added.append(p)
            except Exception as e:
                errors.append({"row": i + 2, "error": str(e)})

        _projects.extend(added)
        return {"added": len(added), "errors": errors, "projects": added}
    except Exception as e:
        raise HTTPException(400, f"Could not parse file: {e}")


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
    labels = [k.replace("_", " ").title() for k in FACTOR_KEYS]
    weights = [RISK_WEIGHTS[k] for k in FACTOR_KEYS]
    return {
        "labels":  labels,
        "keys":    FACTOR_KEYS,
        "matrix":  CORR,
        "weights": weights,
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
