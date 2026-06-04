"""
Claude-backed extraction of project data from spreadsheets or documents.

Requires ANTHROPIC_API_KEY. Optional:
  ANTHROPIC_BASE_URL    (default https://api.anthropic.com)
  ANTHROPIC_IMPORT_MODEL(default claude-sonnet-4-6)
  ANTHROPIC_VERSION     (default 2023-06-01)

Two extraction paths:
  - "table"    : CSV / Excel registers → one project per row.
  - "document" : PDF / text (term sheets, permits, EPC contracts) → project(s) from prose.

Both return the same JSON shape: {"projects": [ {…fields…} ]}.
"""

from __future__ import annotations

import json
import logging
import os
import re
from io import BytesIO
from typing import Any, List, Tuple

import httpx

log = logging.getLogger("meridian.agent.import")

PROVIDER = "anthropic"
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
ANTHROPIC_BASE_URL = os.getenv("ANTHROPIC_BASE_URL", "https://api.anthropic.com").rstrip("/")
ANTHROPIC_IMPORT_MODEL = os.getenv("ANTHROPIC_IMPORT_MODEL", "claude-sonnet-4-6")
ANTHROPIC_VERSION = os.getenv("ANTHROPIC_VERSION", "2023-06-01")

# Core required fields plus optional metadata fields the app can store/display.
REQUIRED_FIELDS = ["name", "type", "state", "mw", "capex", "offtake", "status"]
OPTIONAL_FIELDS = ["developer", "cod", "notes"]


class AgentImportError(Exception):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def agent_import_configured() -> bool:
    return bool(ANTHROPIC_API_KEY)


def agent_import_info() -> dict:
    """Provider/model status for the UI (never exposes the key)."""
    return {
        "enabled": agent_import_configured(),
        "provider": PROVIDER,
        "model": ANTHROPIC_IMPORT_MODEL if agent_import_configured() else None,
    }


# ── File → text ───────────────────────────────────────────────────────────────

def file_to_payload(content: bytes, filename: str) -> Tuple[str, str]:
    """
    Return (text, kind) where kind is "table" or "document".
    Spreadsheets become CSV text; PDFs/plain text become document prose.
    """
    fn = (filename or "").lower()

    if fn.endswith((".xlsx", ".xls")):
        try:
            import pandas as pd
        except ImportError as e:
            raise AgentImportError("pandas is required for Excel import.", 500) from e
        try:
            df = pd.read_excel(BytesIO(content), sheet_name=0, engine=None)
        except Exception as e:
            raise AgentImportError(f"Could not read Excel file: {e}", 400) from e
        return df.iloc[:200, :32].to_csv(index=False), "table"

    if fn.endswith(".pdf"):
        return _pdf_to_text(content), "document"

    # CSV → table; other text (.txt/.md/unknown) → document prose.
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = content.decode("utf-8", errors="replace")

    if fn.endswith(".csv"):
        return "\n".join(text.splitlines()[:300]), "table"
    return text[:200_000], "document"


def _pdf_to_text(content: bytes) -> str:
    try:
        from pypdf import PdfReader
    except ImportError as e:
        raise AgentImportError("pypdf is required for PDF import (pip install pypdf).", 500) from e
    try:
        reader = PdfReader(BytesIO(content))
    except Exception as e:
        raise AgentImportError(f"Could not read PDF file: {e}", 400) from e

    pages = []
    for page in reader.pages[:40]:
        try:
            pages.append(page.extract_text() or "")
        except Exception:
            continue
    text = "\n".join(pages).strip()
    if not text:
        raise AgentImportError(
            "No extractable text found in PDF (it may be scanned/image-only).", 422
        )
    return text[:200_000]


# ── Prompts ─────────────────────────────────────────────────────────────────

_FIELDS_SPEC = """Each project object MUST include:
- name: string (project title)
- type: the project's actual technology type as a short Title Case label. Use one of the
  standard labels when it fits: "Geothermal", "Nuclear SMR", "Battery Storage". If the
  project is a different technology, use a concise label for what it actually is
  (e.g. "Carbon Capture", "Solar PV", "Onshore Wind", "Green Hydrogen", "Pumped Hydro").
- state: two-letter U.S. state code, uppercase
- mw: number (nameplate capacity in MW)
- capex: number (total CapEx in USD — NOT millions; if the source uses $M or $B, convert to dollars)
- offtake: exactly one of: "PPA-Utility", "PPA-Commercial", "Merchant", "Self-Supply"
- status: exactly one of: "Development", "Construction", "Operating", "Decommissioning"

Each project object MAY also include (use null when unknown):
- developer: string (project sponsor / developer / operator / counterparty)
- cod: string (commercial operation date; ISO date or year)
- notes: string (one short sentence summarizing key diligence facts)

Rules:
- Map synonyms to the standard labels (BESS/battery → "Battery Storage"; SMR/nuclear/reactor → "Nuclear SMR").
- Do NOT force an unrelated technology into a standard label — record what the project actually is.
- When offtake/status are unknown, prefer "Merchant" and "Development".
- Output ONLY a single JSON object: {"projects": [ ... ]}. No prose, no markdown fences."""

_SYSTEM_TABLE = (
    "You are an import assistant for Meridian Risk Lab (clean energy project underwriting). "
    "You receive tabular text (CSV). Map EACH data row to one project object.\n\n" + _FIELDS_SPEC
)

_SYSTEM_DOCUMENT = (
    "You are an import assistant for Meridian Risk Lab (clean energy project underwriting). "
    "You receive the text of a project document (term sheet, permit, EPC contract, financing memo, etc.). "
    "Extract the project(s) it describes — usually exactly ONE. Pull values from the prose; "
    "do not invent figures that are absent (use null for optional fields).\n\n" + _FIELDS_SPEC
)


# ── Anthropic call ─────────────────────────────────────────────────────────────

def _call_claude(system: str, user: str, max_tokens: int = 4096) -> str:
    if not ANTHROPIC_API_KEY:
        raise AgentImportError("AI import is not configured (set ANTHROPIC_API_KEY).", 503)

    url = f"{ANTHROPIC_BASE_URL}/v1/messages"
    payload = {
        "model": ANTHROPIC_IMPORT_MODEL,
        "max_tokens": max_tokens,
        "temperature": 0.1,
        "system": system,
        "messages": [{"role": "user", "content": user}],
    }
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": ANTHROPIC_VERSION,
        "content-type": "application/json",
    }

    try:
        with httpx.Client(timeout=120.0) as client:
            r = client.post(url, headers=headers, json=payload)
    except httpx.RequestError as e:
        log.warning("Claude request failed: %s", e)
        raise AgentImportError(f"Could not reach Claude API: {e}", 502) from e

    if r.status_code >= 400:
        log.warning("Claude HTTP %s: %s", r.status_code, r.text[:500])
        detail = "Check ANTHROPIC_API_KEY, model name, and ANTHROPIC_BASE_URL."
        if r.status_code in (401, 403):
            detail = "Authentication failed — verify ANTHROPIC_API_KEY."
        raise AgentImportError(f"Claude API error ({r.status_code}). {detail}", 502)

    try:
        data = r.json()
        blocks = data.get("content", [])
        text = "".join(b.get("text", "") for b in blocks if b.get("type") == "text")
    except (ValueError, AttributeError) as e:
        raise AgentImportError("Unexpected response from Claude API.", 502) from e

    if not text.strip():
        raise AgentImportError("Claude returned an empty response.", 502)
    return text


def verify_agent_credentials() -> dict:
    """Minimal live call to confirm the key/model work. Returns {ok, detail}."""
    if not ANTHROPIC_API_KEY:
        return {"ok": False, "provider": PROVIDER, "detail": "ANTHROPIC_API_KEY is not set."}
    try:
        _call_claude(
            system="You are a connectivity probe. Reply with the single word OK.",
            user="ping",
            max_tokens=8,
        )
        return {"ok": True, "provider": PROVIDER, "model": ANTHROPIC_IMPORT_MODEL,
                "detail": "Connection succeeded."}
    except AgentImportError as e:
        return {"ok": False, "provider": PROVIDER, "detail": e.message}


# ── JSON parsing + mapping ─────────────────────────────────────────────────────

def _balanced_json_span(text: str) -> str | None:
    """Return the substring from the first '{' to its matching '}', ignoring
    braces that appear inside string literals. Tolerates trailing prose."""
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    in_str = False
    escape = False
    for i in range(start, len(text)):
        c = text[i]
        if in_str:
            if escape:
                escape = False
            elif c == "\\":
                escape = True
            elif c == '"':
                in_str = False
        else:
            if c == '"':
                in_str = True
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    return text[start : i + 1]
    return None  # never balanced → likely truncated output


def _extract_json_object(text: str) -> dict[str, Any]:
    text = text.strip()
    # Strip accidental markdown fences.
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n?|\n?```$", "", text.strip())
    candidate = _balanced_json_span(text)
    if candidate is None:
        # Fall back to a greedy span (handles odd cases); may still fail to parse.
        m = re.search(r"\{[\s\S]*\}", text)
        candidate = m.group(0) if m else text
    # strict=False tolerates raw newlines/tabs inside string values (e.g. notes).
    return json.loads(candidate, strict=False)


def _projects_from_text(text: str) -> List[dict[str, Any]]:
    try:
        obj = _extract_json_object(text)
    except json.JSONDecodeError as e:
        log.warning("Claude JSON parse error: %s", text[:1000])
        snippet = text.strip().replace("\n", " ")[:160]
        raise AgentImportError(
            f"Model did not return valid JSON. Response began: {snippet!r}", 502
        ) from e

    projects = obj.get("projects")
    if not isinstance(projects, list):
        raise AgentImportError('Model JSON must include a "projects" array.', 502)
    return [item for item in projects if isinstance(item, dict)]


def map_content_to_projects(text: str, kind: str) -> List[dict[str, Any]]:
    """Map extracted file text to project dicts using the right prompt for its kind."""
    system = _SYSTEM_DOCUMENT if kind == "document" else _SYSTEM_TABLE
    intro = (
        "Extract the project(s) from the following document text into the JSON schema described."
        if kind == "document"
        else "Parse the following table into the JSON schema described."
    )
    content = _call_claude(system=system, user=f"{intro}\n\n---\n{text[:120_000]}", max_tokens=8192)
    return _projects_from_text(content)
