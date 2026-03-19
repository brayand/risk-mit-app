"""
Meridian Risk Lab — Pipeline Orchestrator
Runs all data source connectors in sequence, logs results, handles failures gracefully.
Usage:
    python orchestrator.py              # run all sources
    python orchestrator.py --source eia # run one source
    python orchestrator.py --test       # dry run, no DB writes
"""

import sys
import os
import logging
import argparse
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from config import DB_PATH, LOG_PATH
from schema.db import init_db
from connectors.eia import EIAConnector
from connectors.nrel import NRELConnector
from connectors.usgs import USGSConnector
from connectors.fred_nrc import FREDConnector, NRCConnector

# ── Logging setup ─────────────────────────────────────────────────────────────
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, mode="a"),
    ]
)
log = logging.getLogger("meridian.orchestrator")

# ── Connector registry ────────────────────────────────────────────────────────
CONNECTORS = {
    "nrel":  NRELConnector,     # first — embedded data, always works
    "nrc":   NRCConnector,      # NRC reactor/event data
    "usgs":  USGSConnector,     # seismic — live API
    "fred":  FREDConnector,     # macro indicators — live API
    "eia":   EIAConnector,      # electricity generation/prices — live API
}


def run_pipeline(sources: list = None, dry_run: bool = False) -> dict:
    """
    Run the full data pipeline.
    sources: list of source names to run (None = all)
    dry_run: initialize DB and log but don't persist data
    """
    start_time = time.time()
    log.info("=" * 60)
    log.info(f"Meridian Risk Lab — Pipeline Run")
    log.info(f"Started: {datetime.utcnow().isoformat()}Z")
    log.info(f"Sources: {sources or list(CONNECTORS.keys())}")
    log.info(f"Dry run: {dry_run}")
    log.info("=" * 60)

    # Initialize DB
    conn = init_db(DB_PATH)

    results = {}
    to_run = sources or list(CONNECTORS.keys())

    for source_name in to_run:
        if source_name not in CONNECTORS:
            log.warning(f"Unknown source '{source_name}' — skipping")
            continue

        log.info(f"\n── Running connector: {source_name.upper()} ──")
        connector_class = CONNECTORS[source_name]
        connector = connector_class()

        try:
            if dry_run:
                log.info(f"[{source_name}] DRY RUN — skipping execution")
                results[source_name] = {"status": "dry_run"}
            else:
                result = connector.run(conn)
                results[source_name] = {**result, "status": "success"}
                log.info(f"[{source_name}] ✓ {result.get('rows_inserted', 0)} rows inserted in {result.get('duration_sec', 0):.1f}s")

        except Exception as e:
            log.error(f"[{source_name}] ✗ FAILED: {e}", exc_info=True)
            results[source_name] = {"status": "failed", "error": str(e)}
            # Log failure to DB
            try:
                connector.log_run(conn, "failed", error=str(e))
            except Exception:
                pass

    # ── Summary ───────────────────────────────────────────────────────────────
    total_time = time.time() - start_time
    success = sum(1 for r in results.values() if r.get("status") == "success")
    failed  = sum(1 for r in results.values() if r.get("status") == "failed")
    total_rows = sum(r.get("rows_inserted", 0) for r in results.values())

    log.info("\n" + "=" * 60)
    log.info(f"Pipeline complete in {total_time:.1f}s")
    log.info(f"  Sources: {success} succeeded, {failed} failed")
    log.info(f"  Total rows inserted/updated: {total_rows}")
    log.info("=" * 60)

    # Print DB table counts
    tables = ["eia_generation", "eia_prices", "nrel_atb", "nrc_reactors",
              "nrc_events", "usgs_seismic", "usgs_seismic_summary",
              "fred_indicators", "doe_research", "risk_scores"]
    log.info("\nDatabase table counts:")
    for table in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            log.info(f"  {table:<30} {count:>6} rows")
        except Exception:
            pass

    conn.close()
    return {
        "started_at":    datetime.utcnow().isoformat(),
        "duration_sec":  round(total_time, 1),
        "sources":       results,
        "total_rows":    total_rows,
        "success_count": success,
        "failed_count":  failed,
    }


def main():
    parser = argparse.ArgumentParser(description="Meridian Risk Lab Pipeline")
    parser.add_argument("--source",  nargs="*", help="Specific sources to run")
    parser.add_argument("--test",    action="store_true", help="Dry run")
    parser.add_argument("--verbose", action="store_true", help="Debug logging")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    result = run_pipeline(sources=args.source, dry_run=args.test)
    sys.exit(0 if result["failed_count"] == 0 else 1)


if __name__ == "__main__":
    main()
