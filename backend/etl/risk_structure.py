"""
Risk hierarchy: subcategory scores and correlations roll up to 8 parent categories.

- Subcomponent scores: parent seed + template offset (equal weight within category).
- Category scores: arithmetic mean of subcomponent scores per parent.
- Composite: sum(RISK_WEIGHTS[parent] * category_score[parent]).
- Subcomponent ρ: seeded from FACTOR_BASE_CORR with +0.22 within same parent.
- Category ρ: weighted covariance rollup from the subcomponent matrix (Monte Carlo).
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

FACTOR_KEYS = [
    "technology", "regulatory", "construction", "counterparty",
    "physical", "financial", "operational", "workforce",
]

RISK_WEIGHTS = {
    "technology": 0.22,
    "regulatory": 0.18,
    "construction": 0.15,
    "counterparty": 0.12,
    "physical": 0.11,
    "financial": 0.10,
    "operational": 0.08,
    "workforce": 0.04,
}

FACTOR_BASE_CORR = [
    [1.00, 0.42, 0.31, 0.18, 0.09, 0.25, 0.14, 0.11],
    [0.42, 1.00, 0.27, 0.21, 0.33, 0.19, 0.08, 0.06],
    [0.31, 0.27, 1.00, 0.55, 0.12, 0.61, 0.38, 0.22],
    [0.18, 0.21, 0.55, 1.00, 0.07, 0.44, 0.29, 0.17],
    [0.09, 0.33, 0.12, 0.07, 1.00, 0.08, 0.11, 0.05],
    [0.25, 0.19, 0.61, 0.44, 0.08, 1.00, 0.33, 0.19],
    [0.14, 0.08, 0.38, 0.29, 0.11, 0.33, 1.00, 0.47],
    [0.11, 0.06, 0.22, 0.17, 0.05, 0.19, 0.47, 1.00],
]

WITHIN_PARENT_CORR_BOOST = 0.22

SUBCOMPONENT_TEMPLATES = {
    "technology": [
        ("Tech Maturity", "Technology readiness and proven deployment.", +7),
        ("Design Complexity", "Engineering design complexity and uncertainty.", +4),
        ("Supply Chain Depth", "Critical component supply chain resilience.", +2),
        ("Vendor Concentration", "Dependency on single-source vendors.", 0),
        ("Integration Risk", "System integration coupling risk.", -2),
        ("Performance Variability", "Output and performance variability risk.", -4),
        ("Degradation Profile", "Aging/degradation and lifecycle uncertainty.", -6),
        ("Innovation Risk", "Novel feature risk beyond reference plants.", -8),
    ],
    "regulatory": [
        ("Licensing Complexity", "Permit/NRC review complexity and uncertainty.", +7),
        ("Policy Stability", "State/federal policy and market rule volatility.", +5),
        ("Compliance Burden", "Ongoing compliance and reporting burden.", +3),
        ("Siting Constraints", "Land-use/environmental siting constraint risk.", +1),
        ("Permit Lead Time", "Schedule sensitivity to permit lead times.", -1),
        ("Community Opposition", "Public/legal challenge exposure.", -3),
        ("Interconnection Rules", "Grid interconnection regulatory uncertainty.", -5),
        ("Cross-Agency Coordination", "Multi-agency approval coordination risk.", -7),
    ],
    "construction": [
        ("Schedule Pressure", "Probability of timeline slippage.", +8),
        ("Cost Overrun Risk", "Likelihood of capex overrun versus plan.", +6),
        ("Labor Productivity", "Field productivity and rework risk.", +4),
        ("EPC Counterparty", "EPC contractor execution reliability.", +2),
        ("Site Conditions", "Subsurface/logistics site uncertainty.", 0),
        ("Procurement Timing", "Long-lead equipment timing risk.", -2),
        ("Commissioning Risk", "Startup/commissioning defect risk.", -4),
        ("Change-Order Exposure", "Scope change / variation-order risk.", -6),
    ],
    "counterparty": [
        ("Offtake Credit", "Counterparty credit and default resilience.", +7),
        ("Contract Structure", "Contract terms and merchant exposure.", +5),
        ("Concentration Risk", "Revenue concentration in few counterparties.", +3),
        ("Tenor Mismatch", "Contract tenor mismatch versus asset life.", +1),
        ("Indexation Terms", "Tariff/indexation term uncertainty.", -1),
        ("Collateral Strength", "Collateral and guarantee quality risk.", -3),
        ("Termination Clauses", "Early termination clause risk.", -5),
        ("Dispute Risk", "Commercial dispute / enforcement risk.", -7),
    ],
    "physical": [
        ("Seismic Exposure", "Geologic and seismic hazard sensitivity.", +8),
        ("Climate Exposure", "Weather and long-term climate hazard pressure.", +6),
        ("Flood/Wildfire Exposure", "Acute natural hazard exposure.", +4),
        ("Water Availability", "Water resource and cooling availability risk.", +2),
        ("Thermal Stress", "Heat/cold operating stress risk.", 0),
        ("Geotechnical Stability", "Ground stability and subsidence risk.", -2),
        ("Environmental Incidents", "Environmental incident probability.", -4),
        ("Emergency Access", "Emergency response accessibility risk.", -6),
    ],
    "financial": [
        ("Rate Sensitivity", "Interest-rate and refinancing sensitivity.", +7),
        ("Refinance Risk", "Future refinancing / covenant stress risk.", +5),
        ("Liquidity Buffer", "Working-capital and liquidity stress risk.", +3),
        ("Leverage Pressure", "Debt-service leverage pressure.", +1),
        ("FX/Commodity Linkage", "Indirect commodity/fx linkage risk.", -1),
        ("Inflation Pass-through", "Cost inflation pass-through risk.", -3),
        ("Hedge Effectiveness", "Hedge mismatch and basis risk.", -5),
        ("Capital Access", "Access-to-capital cyclicality risk.", -7),
    ],
    "operational": [
        ("Asset Reliability", "Operational reliability and forced outage risk.", +7),
        ("Dispatch Flexibility", "Operational flexibility under market stress.", +5),
        ("Maintenance Quality", "Maintenance planning and execution risk.", +3),
        ("Control Systems", "Control/automation performance risk.", +1),
        ("Spare Parts Lead Time", "Critical spare parts availability risk.", -1),
        ("Cyber Resilience", "Cyber operations and downtime risk.", -3),
        ("Grid Curtailment", "Curtailment and dispatch constraint risk.", -5),
        ("Outage Recovery", "Recovery-time and restart reliability risk.", -7),
    ],
    "workforce": [
        ("Labor Availability", "Skilled labor availability and retention.", +7),
        ("Safety Culture", "Training/safety culture and incident prevention.", +5),
        ("Training Pipeline", "Talent development pipeline sufficiency.", +3),
        ("Union/Labor Relations", "Labor relations and disruption risk.", +1),
        ("Turnover Risk", "Attrition-driven capability erosion risk.", -1),
        ("Contractor Dependence", "Dependence on third-party contractors.", -3),
        ("Shift Coverage", "Shift coverage and fatigue management risk.", -5),
        ("Specialist Scarcity", "Specialist role scarcity risk.", -7),
    ],
}

SUBCOMPONENT_SPECS: List[Tuple[str, str, str, int]] = [
    (label, parent, desc, offset)
    for parent in FACTOR_KEYS
    for (label, desc, offset) in SUBCOMPONENT_TEMPLATES[parent]
]

TECH_BASE_RISK = {
    "Geothermal": {"technology": 55, "regulatory": 45, "construction": 60, "operational": 50},
    "Nuclear SMR": {"technology": 78, "regulatory": 82, "construction": 75, "operational": 45},
    "Battery Storage": {"technology": 35, "regulatory": 30, "construction": 40, "operational": 42},
}

STATUS_MULT = {
    "Development": 1.25,
    "Construction": 1.40,
    "Operating": 0.75,
    "Decommissioning": 1.10,
}

OFFTAKE_RISK = {
    "PPA-Utility": 30,
    "PPA-Commercial": 48,
    "Merchant": 72,
    "Self-Supply": 55,
}


def compute_parent_seeds(
    technology: str,
    state: str,
    capex: float,
    status: str,
    offtake: str,
    adjustments: Optional[Dict[str, float]] = None,
) -> Dict[str, float]:
    """Category seed scores before subcomponent offsets (0–99)."""
    tech_base = TECH_BASE_RISK.get(
        technology,
        {"technology": 50, "regulatory": 50, "construction": 50, "operational": 50},
    )
    mult = STATUS_MULT.get(status, 1.0)
    physical_seed = 55 if state in {"CA", "NV", "AK", "HI", "WA"} else 40
    financial_seed = 45 + max(0, (capex / 1e9 - 0.5) * 8)
    workforce_seed = {"Nuclear SMR": 65, "Geothermal": 45, "Battery Storage": 35}.get(technology, 45)

    seeds = {
        "technology": tech_base["technology"] * mult,
        "regulatory": tech_base["regulatory"] * mult,
        "construction": tech_base["construction"] * mult,
        "counterparty": OFFTAKE_RISK.get(offtake, 55) * mult,
        "physical": physical_seed * mult,
        "financial": financial_seed * mult,
        "operational": tech_base["operational"] * mult,
        "workforce": workforce_seed * mult,
    }

    adj = adjustments or {}
    for factor, delta in adj.items():
        if factor in seeds:
            seeds[factor] = max(0.0, min(99.0, seeds[factor] + delta))

    return seeds


def build_subcomponent_scores(parent_seeds: Dict[str, float]) -> Dict[str, Dict[str, Any]]:
    subs = {}
    for label, parent, desc, off in SUBCOMPONENT_SPECS:
        val = max(0.0, min(99.0, parent_seeds[parent] + off))
        subs[label] = {
            "score": round(val, 2),
            "parent_factor": parent,
            "description": desc,
        }
    return subs


def rollup_category_scores(subcomponents: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
    """Category score = mean of its subcomponent scores."""
    sums: Dict[str, float] = {k: 0.0 for k in FACTOR_KEYS}
    counts: Dict[str, int] = {k: 0 for k in FACTOR_KEYS}
    for meta in subcomponents.values():
        parent = meta["parent_factor"]
        sums[parent] += float(meta["score"])
        counts[parent] += 1
    return {
        parent: round(sums[parent] / max(counts[parent], 1), 2)
        for parent in FACTOR_KEYS
    }


def composite_from_categories(categories: Dict[str, float]) -> float:
    return round(sum(RISK_WEIGHTS[k] * categories[k] for k in FACTOR_KEYS), 1)


def subcomponent_weights() -> List[float]:
    parent_counts = {p: 0 for p in FACTOR_KEYS}
    parent_by_label = {x[0]: x[1] for x in SUBCOMPONENT_SPECS}
    for label in parent_by_label:
        parent_counts[parent_by_label[label]] += 1
    return [
        round(RISK_WEIGHTS[parent_by_label[l]] / max(parent_counts[parent_by_label[l]], 1), 4)
        for l, _, _, _ in SUBCOMPONENT_SPECS
    ]


def build_subcomponent_correlation_matrix() -> Tuple[List[str], List[List[float]]]:
    labels = [x[0] for x in SUBCOMPONENT_SPECS]
    parent_by_label = {x[0]: x[1] for x in SUBCOMPONENT_SPECS}
    factor_index = {k: i for i, k in enumerate(FACTOR_KEYS)}
    matrix: List[List[float]] = []
    for li in labels:
        row = []
        for lj in labels:
            if li == lj:
                row.append(1.0)
                continue
            pi = parent_by_label[li]
            pj = parent_by_label[lj]
            base = FACTOR_BASE_CORR[factor_index[pi]][factor_index[pj]]
            if pi == pj:
                base = min(0.95, base + WITHIN_PARENT_CORR_BOOST)
            row.append(round(max(0.05, min(0.99, base)), 3))
        matrix.append(row)
    return labels, matrix


def _normalized_block_weights(indices: List[int], weights: List[float]) -> List[float]:
    ws = [weights[i] for i in indices]
    s = sum(ws) or 1.0
    return [w / s for w in ws]


def rollup_category_correlation(
    sub_matrix: List[List[float]],
    labels: Optional[List[str]] = None,
) -> List[List[float]]:
    """Implied ρ between category aggregates from subcomponent matrix."""
    labels = labels or [x[0] for x in SUBCOMPONENT_SPECS]
    parent_by_label = {x[0]: x[1] for x in SUBCOMPONENT_SPECS}
    weights = subcomponent_weights()
    idx_by_parent = {
        p: [i for i, l in enumerate(labels) if parent_by_label[l] == p]
        for p in FACTOR_KEYS
    }
    n = len(FACTOR_KEYS)
    variances: List[float] = []

    for p in FACTOR_KEYS:
        idxs = idx_by_parent[p]
        ws = _normalized_block_weights(idxs, weights)
        var = 0.0
        for ii, wi in enumerate(ws):
            i = idxs[ii]
            for jj, wj in enumerate(ws):
                j = idxs[jj]
                var += wi * wj * sub_matrix[i][j]
        variances.append(max(var, 1e-9))

    cat = [[1.0] * n for _ in range(n)]
    for pi, p in enumerate(FACTOR_KEYS):
        for pj, q in enumerate(FACTOR_KEYS):
            if pi >= pj:
                continue
            idxs_p = idx_by_parent[p]
            idxs_q = idx_by_parent[q]
            ws_p = _normalized_block_weights(idxs_p, weights)
            ws_q = _normalized_block_weights(idxs_q, weights)
            cov = 0.0
            for ii, wi in enumerate(ws_p):
                i = idxs_p[ii]
                for jj, wj in enumerate(ws_q):
                    j = idxs_q[jj]
                    cov += wi * wj * sub_matrix[i][j]
            rho = cov / math.sqrt(variances[pi] * variances[pj])
            rho = round(max(0.05, min(0.99, rho)), 3)
            cat[pi][pj] = rho
            cat[pj][pi] = rho
    return cat


def build_correlation_bundle() -> Dict[str, Any]:
    labels, sub_matrix = build_subcomponent_correlation_matrix()
    parent_by_label = {x[0]: x[1] for x in SUBCOMPONENT_SPECS}
    desc_by_label = {x[0]: x[2] for x in SUBCOMPONENT_SPECS}
    weights = subcomponent_weights()
    category_matrix = rollup_category_correlation(sub_matrix, labels)

    grouped = {}
    for parent in FACTOR_KEYS:
        idxs = [i for i, l in enumerate(labels) if parent_by_label[l] == parent]
        if not idxs:
            continue
        g_labels = [labels[i] for i in idxs]
        grouped[parent] = {
            "parent_factor": parent,
            "labels": g_labels,
            "keys": [g.lower().replace(" ", "_") for g in g_labels],
            "matrix": [[sub_matrix[i][j] for j in idxs] for i in idxs],
            "descriptions": [desc_by_label[l] for l in g_labels],
        }

    return {
        "labels": labels,
        "keys": [l.lower().replace(" ", "_") for l in labels],
        "matrix": sub_matrix,
        "weights": weights,
        "parents": [parent_by_label[l] for l in labels],
        "descriptions": [desc_by_label[l] for l in labels],
        "grouped_matrices": grouped,
        "category_labels": FACTOR_KEYS,
        "category_matrix": category_matrix,
        "category_weights": RISK_WEIGHTS,
    }


_CORR_BUNDLE = build_correlation_bundle()
CATEGORY_CORR: List[List[float]] = _CORR_BUNDLE["category_matrix"]
CORR = CATEGORY_CORR


def score_project_hierarchy(
    technology: str,
    state: str,
    capex: float,
    mw: float,
    status: str,
    offtake: str,
    adjustments: Optional[Dict[str, float]] = None,
    atb_capex_kw: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Seeds → subcomponents → category rollup → composite.
    Returns factor scores, composite, and subcomponents dict.
    """
    seeds = compute_parent_seeds(technology, state, capex, status, offtake, adjustments)

    if atb_capex_kw and mw > 0:
        implied_capex_kw = capex / (mw * 1000)
        ratio = implied_capex_kw / atb_capex_kw
        if ratio > 1.2:
            bump = (ratio - 1.2) * 30
            seeds["technology"] = min(99.0, seeds["technology"] + bump)
            seeds["construction"] = min(99.0, seeds["construction"] + (ratio - 1.2) * 20)

    subs = build_subcomponent_scores(seeds)
    categories = rollup_category_scores(subs)
    composite = composite_from_categories(categories)

    return {
        **categories,
        "composite": composite,
        "subcomponents": subs,
        "category_scores": categories,
    }


def build_project_subcomponents(project: dict) -> dict:
    """Per-project subcomponent scores (aligned with risk engine hierarchy)."""
    scored = score_project_hierarchy(
        technology=project.get("type", "Geothermal"),
        state=project.get("state", ""),
        capex=float(project.get("capex", 0)),
        mw=float(project.get("mw", 0)),
        status=project.get("status", "Development"),
        offtake=project.get("offtake", "Merchant"),
    )
    return scored["subcomponents"]
