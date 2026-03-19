"""
Meridian Risk Lab — Risk Engine
Computes composite risk scores, Monte Carlo loss distributions, and NAIC RBC capital
from live pipeline data for any project or portfolio.
"""

import logging
import sqlite3
import math
import random
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from schema.db import get_conn
from config import DB_PATH

log = logging.getLogger("meridian.risk")

# ── Risk weight configuration (must sum to 1.0) ───────────────────────────────
RISK_WEIGHTS = {
    "technology":    0.22,
    "regulatory":    0.18,
    "construction":  0.15,
    "counterparty":  0.12,
    "physical":      0.11,
    "financial":     0.10,
    "operational":   0.08,
    "workforce":     0.04,
}

# ── Technology base risk profiles (0–100 scale) ───────────────────────────────
TECH_BASE_RISK = {
    "Geothermal":      {"technology": 55, "regulatory": 45, "construction": 60, "operational": 50},
    "Nuclear SMR":     {"technology": 78, "regulatory": 82, "construction": 75, "operational": 45},
    "Battery Storage": {"technology": 35, "regulatory": 30, "construction": 40, "operational": 42},
}

# ── Status risk multipliers ───────────────────────────────────────────────────
STATUS_MULT = {
    "Development":     1.25,
    "Construction":    1.40,
    "Operating":       0.75,
    "Decommissioning": 1.10,
}

# ── NAIC RBC C-factor rates by technology ─────────────────────────────────────
# Based on NAIC Schedule DB and energy sector C-1/C-3 factors
NAIC_C1_FACTOR = {"Geothermal": 0.0080, "Nuclear SMR": 0.0115, "Battery Storage": 0.0062}
NAIC_C3_FACTOR = {"Geothermal": 0.0055, "Nuclear SMR": 0.0070, "Battery Storage": 0.0040}

# ── Correlation matrix (8×8) for Monte Carlo copula ──────────────────────────
FACTOR_KEYS = ["technology", "regulatory", "construction", "counterparty",
               "physical", "financial", "operational", "workforce"]

CORR = [
    [1.00, 0.42, 0.31, 0.18, 0.09, 0.25, 0.14, 0.11],
    [0.42, 1.00, 0.27, 0.21, 0.33, 0.19, 0.08, 0.06],
    [0.31, 0.27, 1.00, 0.55, 0.12, 0.61, 0.38, 0.22],
    [0.18, 0.21, 0.55, 1.00, 0.07, 0.44, 0.29, 0.17],
    [0.09, 0.33, 0.12, 0.07, 1.00, 0.08, 0.11, 0.05],
    [0.25, 0.19, 0.61, 0.44, 0.08, 1.00, 0.33, 0.19],
    [0.14, 0.08, 0.38, 0.29, 0.11, 0.33, 1.00, 0.47],
    [0.11, 0.06, 0.22, 0.17, 0.05, 0.19, 0.47, 1.00],
]


def cholesky(matrix: List[List[float]]) -> List[List[float]]:
    """Pure-Python Cholesky decomposition for correlation matrix."""
    n = len(matrix)
    L = [[0.0] * n for _ in range(n)]
    for i in range(n):
        for j in range(i + 1):
            s = sum(L[i][k] * L[j][k] for k in range(j))
            if i == j:
                val = matrix[i][i] - s
                L[i][j] = math.sqrt(max(val, 1e-10))
            else:
                L[i][j] = (matrix[i][j] - s) / L[j][j] if L[j][j] > 1e-10 else 0.0
    return L


def box_muller() -> Tuple[float, float]:
    """Generate two standard normal samples via Box-Muller transform."""
    u1 = max(random.random(), 1e-12)
    u2 = random.random()
    z0 = math.sqrt(-2 * math.log(u1)) * math.cos(2 * math.pi * u2)
    z1 = math.sqrt(-2 * math.log(u1)) * math.sin(2 * math.pi * u2)
    return z0, z1


def normal_cdf(x: float) -> float:
    """Approximation of the standard normal CDF."""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


class RiskEngine:

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._chol = cholesky(CORR)

    def _get_db_adjustments(self, technology: str, state: str) -> Dict[str, float]:
        """Pull live adjustments from pipeline data."""
        adjustments = {}
        try:
            conn = get_conn(self.db_path)

            # Seismic risk (physical factor)
            row = conn.execute(
                "SELECT risk_multiplier FROM usgs_seismic_summary WHERE state=?", (state,)
            ).fetchone()
            if row:
                adjustments["physical"] = min((row["risk_multiplier"] - 1.0) * 20, 30)

            # FRED: interest rate effect on financial risk
            row = conn.execute("""
                SELECT value FROM fred_indicators
                WHERE series_id='DGS10' ORDER BY period DESC LIMIT 1
            """).fetchone()
            if row:
                rate = row["value"]
                # Rates above 4.5% add financial risk, below 3% reduce it
                adjustments["financial"] = (rate - 4.0) * 5.0

            # FRED: construction cost index
            row = conn.execute("""
                SELECT value FROM fred_indicators
                WHERE series_id='WPUSI012011' ORDER BY period DESC LIMIT 1
            """).fetchone()
            if row:
                # Above 200 = elevated construction cost risk
                adjustments["construction"] = max((row["value"] - 200) * 0.1, 0)

            # NRC regulatory events (nuclear only)
            if technology == "Nuclear SMR":
                row = conn.execute("""
                    SELECT COUNT(*) as cnt FROM nrc_events
                    WHERE state=? AND severity >= 3
                    AND event_date >= date('now', '-180 days')
                """, (state,)).fetchone()
                if row and row["cnt"]:
                    adjustments["regulatory"] = row["cnt"] * 5.0

            # NREL: capex vs benchmark (construction/technology risk)
            tech_map = {"Nuclear SMR": "Nuclear", "Geothermal": "Geothermal",
                        "Battery Storage": "Battery Storage"}
            atb_tech = tech_map.get(technology)
            if atb_tech:
                row = conn.execute("""
                    SELECT capex_per_kw FROM nrel_atb
                    WHERE technology=? AND scenario='Moderate'
                    ORDER BY year DESC LIMIT 1
                """, (atb_tech,)).fetchone()
                if row:
                    adjustments["_atb_capex_kw"] = row["capex_per_kw"]

            conn.close()
        except Exception as e:
            log.warning(f"[risk] DB adjustment error: {e}")

        return adjustments

    def score_project(self, technology: str, state: str, capex: float,
                      mw: float, status: str, offtake: str) -> Dict:
        """
        Compute all risk factor scores for a single project.
        Returns dict with individual scores + composite.
        """
        base = TECH_BASE_RISK.get(technology, TECH_BASE_RISK["Battery Storage"]).copy()
        adj  = self._get_db_adjustments(technology, state)

        # Fill defaults
        scores = {k: base.get(k, 50.0) for k in FACTOR_KEYS}

        # Counterparty risk from offtake structure
        offtake_risk = {"PPA-Utility": 30, "PPA-Commercial": 48,
                        "Merchant": 72, "Self-Supply": 55}
        scores["counterparty"] = offtake_risk.get(offtake, 55)

        # Workforce risk by technology
        scores["workforce"] = {"Nuclear SMR": 65, "Geothermal": 45, "Battery Storage": 35}.get(technology, 45)

        # Financial risk base
        scores["financial"] = 45 + max(0, (capex / 1e9 - 0.5) * 8)  # scale with capex size

        # Apply live data adjustments
        for factor, delta in adj.items():
            if factor in scores:
                scores[factor] = max(0, min(99, scores[factor] + delta))

        # Apply status multiplier
        mult = STATUS_MULT.get(status, 1.0)
        scores = {k: min(99, v * mult) for k, v in scores.items()}

        # Composite weighted score
        composite = sum(RISK_WEIGHTS[k] * scores[k] for k in FACTOR_KEYS)

        # CapEx vs ATB benchmark — add technology/construction risk if over budget
        atb_capex = adj.get("_atb_capex_kw")
        if atb_capex and mw > 0:
            implied_capex_kw = capex / (mw * 1000)
            ratio = implied_capex_kw / atb_capex
            if ratio > 1.2:
                scores["technology"]    = min(99, scores["technology"] + (ratio - 1.2) * 30)
                scores["construction"]  = min(99, scores["construction"] + (ratio - 1.2) * 20)
                composite = sum(RISK_WEIGHTS[k] * scores[k] for k in FACTOR_KEYS)

        return {**scores, "composite": round(composite, 1)}

    def monte_carlo(self, project_scores: Dict, capex: float,
                    n_trials: int = 10000) -> Dict:
        """
        Run Monte Carlo simulation using correlated loss factors.
        Returns loss statistics: EL, VaR95, VaR99, TVaR99, PML.
        """
        composite = project_scores["composite"]

        # Annual frequency: Poisson lambda derived from composite score
        lam = 0.05 + (composite / 100) ** 2 * 0.45

        # Severity: log-normal params derived from capex and risk score
        sev_mean = capex * (0.01 + composite / 100 * 0.12)  # 1%–13% of capex
        sev_cv   = 0.6 + composite / 100 * 0.8               # 60%–140% CV

        mu_ln = math.log(sev_mean) - 0.5 * math.log(1 + sev_cv ** 2)
        sig_ln = math.sqrt(math.log(1 + sev_cv ** 2))

        losses = []
        random.seed(42)

        for _ in range(n_trials):
            # Correlated standard normals via Cholesky
            indep = []
            for i in range(0, len(FACTOR_KEYS), 2):
                z0, z1 = box_muller()
                indep.extend([z0, z1])
            indep = indep[:len(FACTOR_KEYS)]
            corr_z = [sum(self._chol[i][j] * indep[j]
                          for j in range(len(FACTOR_KEYS)))
                      for i in range(len(FACTOR_KEYS))]

            # Convert to uniform via normal CDF
            u = [normal_cdf(z) for z in corr_z]

            # Aggregate severity modifier from correlated uniforms
            agg_u = sum(u) / len(u)

            # Poisson frequency
            p = random.random()
            freq = 0
            cumprob = math.exp(-lam)
            while p > cumprob and freq < 10:
                freq += 1
                cumprob += (math.exp(-lam) * lam ** freq /
                            math.factorial(freq))

            # Total annual loss
            total = 0.0
            for _ in range(freq):
                # Correlated log-normal severity
                z = math.sqrt(-2 * math.log(max(random.random(), 1e-12))) * \
                    math.cos(2 * math.pi * random.random())
                z_adj = z * (0.5 + agg_u * 0.5)   # correlation adjustment
                severity = math.exp(mu_ln + sig_ln * z_adj)
                total += max(0, severity)

            losses.append(total)

        losses.sort()
        n = len(losses)

        el    = sum(losses) / n
        var95 = losses[int(n * 0.95)]
        var99 = losses[int(n * 0.99)]
        tail  = losses[int(n * 0.99):]
        tvar99 = sum(tail) / len(tail) if tail else var99
        pml    = losses[int(n * 0.995)]

        return {
            "eal":    round(el, 0),
            "var_95": round(var95, 0),
            "var_99": round(var99, 0),
            "tvar_99": round(tvar99, 0),
            "pml":    round(pml, 0),
        }

    def naic_rbc(self, technology: str, capex: float, loss_stats: Dict) -> Dict:
        """Compute NAIC RBC capital components C-1 through C-4."""
        c1 = capex * NAIC_C1_FACTOR.get(technology, 0.008)
        c3 = capex * NAIC_C3_FACTOR.get(technology, 0.005)
        c2 = loss_stats["eal"] * 0.075          # insurance risk charge
        c4 = capex * 0.001                       # business risk (flat)

        # Covariance formula: RBC = sqrt(C1^2 + C3^2) + C2 + C4
        rbc_total = math.sqrt(c1 ** 2 + c3 ** 2) + c2 + c4

        # Action level thresholds (ratio of actual surplus to RBC)
        return {
            "c1": round(c1, 0), "c2": round(c2, 0),
            "c3": round(c3, 0), "c4": round(c4, 0),
            "total": round(rbc_total, 0),
        }

    def analyze_project(self, project: Dict) -> Dict:
        """
        Full analysis pipeline for one project dict.
        project keys: id, name, type, state, mw, capex, offtake, status
        """
        scores   = self.score_project(
            technology=project["type"], state=project["state"],
            capex=project["capex"],    mw=project["mw"],
            status=project["status"],  offtake=project["offtake"],
        )
        losses   = self.monte_carlo(scores, project["capex"])
        rbc      = self.naic_rbc(project["type"], project["capex"], losses)

        return {
            "project_id":    project.get("id", project["name"]),
            "scores":        scores,
            "losses":        losses,
            "naic_rbc":      rbc,
            "computed_at":   datetime.utcnow().isoformat(),
        }

    def analyze_portfolio(self, projects: List[Dict]) -> Dict:
        """Analyze all projects and aggregate with diversification benefit."""
        results = [self.analyze_project(p) for p in projects]

        # Portfolio aggregation with correlation diversification
        total_eal  = sum(r["losses"]["eal"] for r in results)
        total_var99 = sum(r["losses"]["var_99"] for r in results)
        total_rbc  = sum(r["naic_rbc"]["total"] for r in results)

        # Diversification benefit: Herfindahl-based (more projects = more benefit)
        n = len(projects)
        div_benefit = min(0.05 + 0.03 * math.log(n + 1), 0.25) if n > 1 else 0.0
        diversified_var99 = total_var99 * (1 - div_benefit)
        diversified_rbc   = total_rbc   * (1 - div_benefit * 0.5)

        return {
            "projects":              results,
            "total_eal":             round(total_eal, 0),
            "total_var99_gross":     round(total_var99, 0),
            "total_var99_net":       round(diversified_var99, 0),
            "diversification_pct":   round(div_benefit * 100, 1),
            "total_rbc_gross":       round(total_rbc, 0),
            "total_rbc_net":         round(diversified_rbc, 0),
            "project_count":         n,
        }

    def save_scores(self, conn: sqlite3.Connection, result: Dict):
        """Persist risk scores to DB."""
        s = result["scores"]
        l = result["losses"]
        r = result["naic_rbc"]
        conn.execute("""
            INSERT INTO risk_scores
                (project_id, computed_at, technology_risk, regulatory_risk,
                 construction_risk, counterparty_risk, physical_risk,
                 financial_risk, operational_risk, composite_score,
                 eal_usd, var_95_usd, var_99_usd, tvar_99_usd,
                 naic_c1, naic_c3, data_version)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(project_id, computed_at) DO NOTHING
        """, (
            result["project_id"], result["computed_at"],
            s["technology"], s["regulatory"], s["construction"],
            s["counterparty"], s["physical"], s["financial"], s["operational"],
            s["composite"], l["eal"], l["var_95"], l["var_99"], l["tvar_99"],
            r["c1"], r["c3"], "v1.0"
        ))
        conn.commit()
