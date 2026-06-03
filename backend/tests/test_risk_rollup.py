"""Unit checks for subcategory → category score and correlation rollup."""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from etl.risk_structure import (
    FACTOR_KEYS,
    RISK_WEIGHTS,
    build_correlation_bundle,
    build_subcomponent_scores,
    composite_from_categories,
    compute_parent_seeds,
    rollup_category_correlation,
    rollup_category_scores,
    score_project_hierarchy,
    build_subcomponent_correlation_matrix,
)
from etl.risk_engine import RiskEngine, CATEGORY_CORR
from config import DB_PATH

PASS = "OK"
FAIL = "FAIL"
results = []


def check(name, condition, detail=""):
    status = PASS if condition else FAIL
    results.append((status, name, detail))
    print(f"  {status} {name}" + (f" — {detail}" if detail else ""))
    return condition


print("\n[Risk rollup]")
seeds = compute_parent_seeds("Geothermal", "NV", 142_000_000, "Operating", "PPA-Utility")
subs = build_subcomponent_scores(seeds)
cats = rollup_category_scores(subs)

for parent in FACTOR_KEYS:
    parent_subs = [float(s["score"]) for s in subs.values() if s["parent_factor"] == parent]
    expected_mean = sum(parent_subs) / len(parent_subs)
    check(
        f"category[{parent}] = mean(subs)",
        abs(cats[parent] - round(expected_mean, 2)) < 0.01,
        f"{cats[parent]:.2f} vs {expected_mean:.2f}",
    )

composite = composite_from_categories(cats)
weighted = sum(RISK_WEIGHTS[k] * cats[k] for k in FACTOR_KEYS)
check("composite = weighted categories", abs(composite - round(weighted, 1)) < 0.05, f"{composite}")

h = score_project_hierarchy("Nuclear SMR", "ID", 890_000_000, 77, "Construction", "Merchant")
check("hierarchy returns composite", "composite" in h and 20 <= h["composite"] <= 99)
check(
    "hierarchy category matches rollup",
    all(abs(h[k] - h["category_scores"][k]) < 0.01 for k in FACTOR_KEYS),
)

labels, sub_mat = build_subcomponent_correlation_matrix()
rolled = rollup_category_correlation(sub_mat, labels)
bundle = build_correlation_bundle()
check("bundle category_matrix matches rollup", rolled == bundle["category_matrix"])
check("engine uses rolled CATEGORY_CORR", CATEGORY_CORR == bundle["category_matrix"])

engine = RiskEngine(DB_PATH)
smr = {"id": "t1", "name": "Test SMR", "type": "Nuclear SMR", "state": "ID",
       "mw": 77, "capex": 890_000_000, "offtake": "Merchant", "status": "Construction"}
geo = {"id": "t2", "name": "Test Geo", "type": "Geothermal", "state": "NV",
       "mw": 45, "capex": 142_000_000, "offtake": "PPA-Utility", "status": "Operating"}
bess = {"id": "t3", "name": "Test BESS", "type": "Battery Storage", "state": "TX",
        "mw": 200, "capex": 78_000_000, "offtake": "Merchant", "status": "Operating"}

rs = engine.analyze_project(smr)
rg = engine.analyze_project(geo)
rb = engine.analyze_project(bess)
check("BESS < Geo composite", rb["scores"]["composite"] < rg["scores"]["composite"],
      f"{rb['scores']['composite']} vs {rg['scores']['composite']}")
check("Geo < SMR composite", rg["scores"]["composite"] < rs["scores"]["composite"],
      f"{rg['scores']['composite']} vs {rs['scores']['composite']}")
check("scores include subcomponents", "subcomponents" in rs["scores"] and len(rs["scores"]["subcomponents"]) == 64)

failed = sum(1 for r in results if r[0] == FAIL)
print(f"\n  Rollup tests: {len(results) - failed}/{len(results)} passed")
sys.exit(0 if failed == 0 else 1)
