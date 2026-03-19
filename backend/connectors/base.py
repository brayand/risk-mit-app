"""
Meridian Risk Lab — Base HTTP Connector
Handles retries, backoff, timeout, and structured logging for all source connectors.
"""

import time
import logging
import requests
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import REQUEST_TIMEOUT, MAX_RETRIES, RETRY_BACKOFF

log = logging.getLogger("meridian.http")


class ConnectorError(Exception):
    """Raised when a connector fails after all retries."""
    pass


class BaseConnector:
    """
    Base class for all data source connectors.
    Provides: GET/POST with retry, structured error logging, response caching hint.
    """

    SOURCE_NAME = "base"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "MeridianRiskLab/1.0 (Clean Energy Risk Modeling; contact@meridianrisk.com)",
            "Accept": "application/json",
        })
        self._request_count = 0
        self._last_request_time = 0.0

    def _throttle(self, min_interval: float = 0.5):
        """Simple per-connector rate limiter."""
        elapsed = time.time() - self._last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

    def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = REQUEST_TIMEOUT,
        min_interval: float = 0.5,
    ) -> Any:
        """
        HTTP GET with exponential backoff retry.
        Returns parsed JSON or raises ConnectorError.
        """
        self._throttle(min_interval)
        last_exc = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                log.debug(f"[{self.SOURCE_NAME}] GET {url} params={params} (attempt {attempt})")
                resp = self.session.get(url, params=params, headers=headers, timeout=timeout)
                self._last_request_time = time.time()
                self._request_count += 1

                if resp.status_code == 429:
                    wait = RETRY_BACKOFF ** attempt * 5
                    log.warning(f"[{self.SOURCE_NAME}] Rate limited — waiting {wait:.1f}s")
                    time.sleep(wait)
                    continue

                if resp.status_code == 404:
                    log.warning(f"[{self.SOURCE_NAME}] 404 at {url}")
                    return None

                resp.raise_for_status()

                try:
                    return resp.json()
                except Exception:
                    return resp.text

            except requests.exceptions.Timeout:
                last_exc = ConnectorError(f"Timeout after {timeout}s on attempt {attempt}")
                log.warning(f"[{self.SOURCE_NAME}] Timeout on attempt {attempt}/{MAX_RETRIES}")
            except requests.exceptions.ConnectionError as e:
                last_exc = ConnectorError(f"Connection error: {e}")
                log.warning(f"[{self.SOURCE_NAME}] Connection error attempt {attempt}/{MAX_RETRIES}: {e}")
            except requests.exceptions.HTTPError as e:
                last_exc = ConnectorError(f"HTTP {resp.status_code}: {e}")
                log.error(f"[{self.SOURCE_NAME}] HTTP error {resp.status_code}: {url}")
                break  # don't retry on 4xx (except 429 above)
            except Exception as e:
                last_exc = ConnectorError(f"Unexpected error: {e}")
                log.error(f"[{self.SOURCE_NAME}] Unexpected error: {e}")

            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF ** attempt
                log.info(f"[{self.SOURCE_NAME}] Retrying in {wait:.1f}s...")
                time.sleep(wait)

        raise last_exc or ConnectorError(f"Failed after {MAX_RETRIES} attempts: {url}")

    @staticmethod
    def now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def log_run(self, conn, status: str, rows_in: int = 0, rows_up: int = 0,
                error: str = None, duration: float = 0.0):
        """Record this pipeline run in the pipeline_runs table."""
        conn.execute("""
            INSERT INTO pipeline_runs (run_at, source, status, rows_inserted, rows_updated, error_msg, duration_sec)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (self.now_iso(), self.SOURCE_NAME, status, rows_in, rows_up, error, round(duration, 2)))
        conn.commit()
