"""
Meridian Risk Lab — Integration Test
Tests: DB schema, all connectors, risk engine, full pipeline run.
"""

import sys, os, logging
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from schema.db import init_db, get_conn
from connectors.nrel import NRELConnector
from connectors.usgs import USGSConnector
from connectors.fred_nrc import FREDConnector, NRCConnector
from etl.risk_engine import RiskEngine
from orchestrator import run_pipeline
from config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
log = logging.getLogger("meridian.test")

TEST_PROJECTS = [
    {"id": "p1", "name": "Nevada Geothermal Basin Alpha", "type": "Geothermal",
     "state": "NV", "mw": 45,  "capex": 142_000_000, "offtake": "PPA-Utility",   "status": "Operating"},
    {"id": "p2", "name": "Idaho SMR Pilot Unit 1",        "type": "Nuclear SMR",
     "state": "ID", "mw": 77,  "capex": 890_000_000, "offtake": "Merchant",       "status": "Construction"},
    {"id": "p3", "name": "Texas Grid-Scale BESS",          "type": "Battery Storage",
     "state": "TX", "mw": 200, "capex":  78_000_000, "offtake": "Merchant",       "status": "Operating"},
    {"id": "p4", "name": "Oregon Cascade Geothermal",      "type": "Geothermal",
     "state": "OR", "mw": 30,  "capex":  98_000_000, "offtake": "PPA-Commercial", "status": "Development"},
    {"id": "p5", "name": "Wyoming SMR Cluster",            "type": "Nuclear SMR",
     "state": "WY", "mw": 154, "capex": 1_640_000_000,"offtake": "PPA-Utility",  "status": "Development"},
]

PASS = "✓"
FAIL = "✗"
results = []

def check(name, condition, detail=""):
    status = PASS if condition else FAIL
    results.append((status, name, detail))
    print(f"  {status} {name}" + (f" — {detail}" if detail else ""))
    return condition


print("\n" + "=" * 65)
print("  Meridian Risk Lab — Integration Test Suite")
print("=" * 65)

# ── 1. Database schema ────────────────────────────────────────────────────────
print("\n[1] Database Schema")
conn = init_db(DB_PATH)
tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
check("pipeline_runs table",       "pipeline_runs" in tables)
check("nrel_atb table",            "nrel_atb" in tables)
check("usgs_seismic table",        "usgs_seismic" in tables)
check("fred_indicators table",     "fred_indicators" in tables)
check("risk_scores table",         "risk_scores" in tables)
check("usgs_seismic_summary table","usgs_seismic_summary" in tables)

# ── 2. NREL connector (embedded data — always works) ─────────────────────────
print("\n[2] NREL ATB Connector")
nrel = NRELConnector()
res = nrel.run(conn)
check("NREL run succeeded",   res.get("rows_inserted", 0) > 0, f"{res.get('rows_inserted')} rows")
geo  = nrel.get_cost_benchmark(conn, "Geothermal", "Moderate", 2024)
check("Geothermal 2024 benchmark", geo is not None and geo["capex_per_kw"] > 0,
      f"${geo['capex_per_kw']:,}/kW" if geo else "missing")
nuc  = nrel.get_cost_benchmark(conn, "Nuclear", "Moderate", 2024)
check("Nuclear 2024 benchmark",    nuc is not None, f"${nuc['capex_per_kw']:,}/kW" if nuc else "missing")
bat  = nrel.get_cost_benchmark(conn, "Battery Storage", "Moderate", 2024)
check("Battery Storage benchmark", bat is not None, f"${bat['capex_per_kw']:,}/kW" if bat else "missing")

# ── 3. NRC connector ──────────────────────────────────────────────────────────
print("\n[3] NRC Reactor & Event Data")
nrc = NRCConnector()
res = nrc.run(conn)
check("NRC run succeeded",    res.get("rows_inserted", 0) > 0, f"{res.get('rows_inserted')} rows")
smr_count = conn.execute("SELECT COUNT(*) FROM nrc_reactors WHERE reactor_type='SMR'").fetchone()[0]
check("SMR applications loaded", smr_count >= 5, f"{smr_count} SMR applications")
event_count = conn.execute("SELECT COUNT(*) FROM nrc_events").fetchone()[0]
check("NRC events loaded", event_count >= 3, f"{event_count} events")
wy_risk = nrc.get_smr_regulatory_risk(conn, "WY")
check("WY SMR regulatory risk score", 50 <= wy_risk <= 99, f"score={wy_risk}")

# ── 4. USGS connector (live API) ──────────────────────────────────────────────
print("\n[4] USGS Seismic Connector")
usgs = USGSConnector()
try:
    res = usgs.run(conn)
    check("USGS run completed",       res.get("duration_sec", 0) > 0)
    check("Events fetched",           res.get("rows_inserted", 0) >= 0,
          f"{res.get('rows_inserted', 0)} new events")
    check("State summaries created",  res.get("states_updated", 0) >= 48,
          f"{res.get('states_updated', 0)} states")
    nv_row = conn.execute("SELECT * FROM usgs_seismic_summary WHERE state='NV'").fetchone()
    check("Nevada seismic summary",   nv_row is not None,
          f"zone={nv_row['seismic_zone']}, mult={nv_row['risk_multiplier']}" if nv_row else "missing")
except Exception as e:
    check("USGS connector", False, f"Error: {e}")

# ── 5. FRED connector (live API) ──────────────────────────────────────────────
print("\n[5] FRED Macro Connector")
fred = FREDConnector()
try:
    res = fred.run(conn)
    check("FRED run completed",    res.get("rows_inserted", 0) >= 0,
          f"{res.get('rows_inserted', 0)} rows")
    rate = fred.get_latest(conn, "DGS10")
    check("10-Year Treasury fetched", rate is not None, f"{rate}%" if rate else "not fetched")
    ppi = fred.get_latest(conn, "PPIFES")
    check("PPI Energy fetched",    ppi is not None, f"{ppi}" if ppi else "not fetched (demo key)")
except Exception as e:
    check("FRED connector", False, f"Error: {e}")

# ── 6. Risk Engine ────────────────────────────────────────────────────────────
print("\n[6] Risk Engine")
engine = RiskEngine(DB_PATH)

# Single project
p = TEST_PROJECTS[1]  # Idaho SMR — highest risk
result = engine.analyze_project(p)
check("SMR project analyzed",      "scores" in result)
check("Composite score in range",  20 <= result["scores"]["composite"] <= 99,
      f"score={result['scores']['composite']}")
check("EAL computed",              result["losses"]["eal"] > 0,
      f"EAL=${result['losses']['eal']:,.0f}")
check("VaR 99% > EAL",            result["losses"]["var_99"] > result["losses"]["eal"],
      f"VaR99=${result['losses']['var_99']:,.0f}")
check("NAIC RBC computed",        result["naic_rbc"]["total"] > 0,
      f"RBC=${result['naic_rbc']['total']:,.0f}")

# Geothermal should have lower risk than SMR
pg = TEST_PROJECTS[0]  # Nevada Geothermal
rg = engine.analyze_project(pg)
check("Geothermal < SMR risk",    rg["scores"]["composite"] < result["scores"]["composite"],
      f"Geo={rg['scores']['composite']:.1f} vs SMR={result['scores']['composite']:.1f}")

# Battery Storage should have lowest risk
pb = TEST_PROJECTS[2]  # Texas BESS
rb = engine.analyze_project(pb)
check("Storage < Geothermal risk", rb["scores"]["composite"] < rg["scores"]["composite"],
      f"Storage={rb['scores']['composite']:.1f} vs Geo={rg['scores']['composite']:.1f}")

# Portfolio
portfolio = engine.analyze_portfolio(TEST_PROJECTS)
check("Portfolio analyzed",          "projects" in portfolio and len(portfolio["projects"]) == 5)
check("Diversification benefit",     portfolio["diversification_pct"] > 0,
      f"{portfolio['diversification_pct']}%")
check("Portfolio VaR net < gross",   portfolio["total_var99_net"] < portfolio["total_var99_gross"])

# Save to DB
engine.save_scores(conn, result)
saved = conn.execute("SELECT COUNT(*) FROM risk_scores").fetchone()[0]
check("Scores persisted to DB",     saved >= 1, f"{saved} records")

# ── 7. Full pipeline run ──────────────────────────────────────────────────────
print("\n[7] Full Pipeline Orchestration")
pipeline_result = run_pipeline(sources=["nrel", "nrc"], dry_run=False)
check("Pipeline completed",       pipeline_result["failed_count"] == 0,
      f"{pipeline_result['success_count']} sources OK")
check("Pipeline timing recorded", pipeline_result["duration_sec"] > 0,
      f"{pipeline_result['duration_sec']}s")

run_count = conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0]
check("Pipeline runs logged",     run_count >= 1, f"{run_count} runs in log")

conn.close()

# ── Summary ───────────────────────────────────────────────────────────────────
passed = sum(1 for r in results if r[0] == PASS)
failed = sum(1 for r in results if r[0] == FAIL)
total  = len(results)

print("\n" + "=" * 65)
print(f"  Results: {passed}/{total} passed, {failed} failed")
print("=" * 65)
if failed > 0:
    print("\nFailed checks:")
    for s, n, d in results:
        if s == FAIL:
            print(f"  ✗ {n}" + (f" — {d}" if d else ""))

print()
sys.exit(0 if failed == 0 else 1)
