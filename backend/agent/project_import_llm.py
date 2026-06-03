"""
LLM-backed mapping of spreadsheet snippets → Meridian project rows.
Requires OPENAI_API_KEY. Optional OPENAI_BASE_URL (default OpenAI), OPENAI_IMPORT_MODEL.
"""

from __future__ import annotations

import json
import logging
import os
import re
from io import BytesIO
from typing import Any

import httpx

log = logging.getLogger("meridian.agent.import")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
OPENAI_IMPORT_MODEL = os.getenv("OPENAI_IMPORT_MODEL", "gpt-4o-mini")


class AgentImportError(Exception):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def agent_import_configured() -> bool:
    return bool(OPENAI_API_KEY)


def file_to_tabular_snippet(content: bytes, filename: str) -> str:
    """Produce a compact text snippet for the model (CSV-ish, bounded size)."""
    fn = (filename or "").lower()
    if fn.endswith(".xlsx") or fn.endswith(".xls"):
        try:
            import pandas as pd
        except ImportError as e:
            raise AgentImportError("pandas is required for Excel import.", 500) from e
        try:
            df = pd.read_excel(BytesIO(content), sheet_name=0, engine=None)
        except Exception as e:
            raise AgentImportError(f"Could not read Excel file: {e}", 400) from e
        df = df.iloc[:60, :24]
        return df.to_csv(index=False)

    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = content.decode("utf-8", errors="replace")
    lines = text.splitlines()
    return "\n".join(lines[:150])


_SYSTEM = """You are an import assistant for Meridian Risk Lab (clean energy project underwriting).
You receive tabular text (CSV). Map each data row to a project object.

Output a single JSON object with key "projects" (array). Each element must have:
- name: string (project title)
- type: exactly one of: "Geothermal", "Nuclear SMR", "Battery Storage"
- state: two-letter U.S. state code uppercase
- mw: number (nameplate MW)
- capex: number (total CapEx in USD, not millions — if the sheet uses $M multiply accordingly)
- offtake: exactly one of: "PPA-Utility", "PPA-Commercial", "Merchant", "Self-Supply"
- status: exactly one of: "Development", "Construction", "Operating", "Decommissioning"

Infer missing offtake/status with best judgment from context; prefer "Merchant" and "Development" when unknown.
Map synonyms (e.g. BESS/battery storage → Battery Storage, SMR/nuclear → Nuclear SMR).
Ignore completely empty rows. Do not include commentary outside JSON."""


def _extract_json_object(text: str) -> dict[str, Any]:
    text = text.strip()
    m = re.search(r"\{[\s\S]*\}\s*$", text)
    if m:
        text = m.group(0)
    return json.loads(text)


def map_snippet_to_projects(snippet: str) -> list[dict[str, Any]]:
    if not OPENAI_API_KEY:
        raise AgentImportError("AI import is not configured (set OPENAI_API_KEY).", 503)

    url = f"{OPENAI_BASE_URL}/chat/completions"
    payload = {
        "model": OPENAI_IMPORT_MODEL,
        "temperature": 0.1,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _SYSTEM},
            {
                "role": "user",
                "content": "Parse the following table into the JSON schema described.\n\n---\n"
                + snippet[:120_000],
            },
        ],
    }
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    try:
        with httpx.Client(timeout=120.0) as client:
            r = client.post(url, headers=headers, json=payload)
    except httpx.RequestError as e:
        log.warning("LLM request failed: %s", e)
        raise AgentImportError(f"Could not reach model API: {e}", 502) from e

    if r.status_code >= 400:
        log.warning("LLM HTTP %s: %s", r.status_code, r.text[:500])
        raise AgentImportError(
            "Model API returned an error. Check OPENAI_API_KEY and OPENAI_BASE_URL.",
            502,
        )

    try:
        data = r.json()
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, json.JSONDecodeError) as e:
        raise AgentImportError("Unexpected response from model API.", 502) from e

    try:
        obj = _extract_json_object(content)
    except json.JSONDecodeError as e:
        log.warning("LLM JSON parse error: %s", content[:300])
        raise AgentImportError("Model did not return valid JSON.", 502) from e

    projects = obj.get("projects")
    if not isinstance(projects, list):
        raise AgentImportError('Model JSON must include a "projects" array.', 502)

    out: list[dict[str, Any]] = []
    for item in projects:
        if isinstance(item, dict):
            out.append(item)
    return out
