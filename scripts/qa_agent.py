"""
qa_agent.py — Stage 5 of the Gaceta del Congreso pipeline.

Structural QA checks on a batch of speeches and sessions.
Appends anomalies to qa/flagged.csv.

No LLM calls in this version — structural checks only.
LLM diagnosis can be added later once flagged.csv accumulates cases.
"""

import csv
import os
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
FLAGGED_CSV = PROJECT_ROOT / "qa" / "flagged.csv"

FLAGGED_FIELDNAMES = [
    "file_id", "speech_id", "page_num", "year_extracted",
    "failure_type", "severity", "original_excerpt",
    "diagnosis", "rule_version", "resolved",
]


def _append_flag(rows: list[dict]) -> None:
    if not rows:
        return
    FLAGGED_CSV.parent.mkdir(parents=True, exist_ok=True)
    existing_keys: set[tuple] = set()
    write_header = not FLAGGED_CSV.exists()
    if not write_header:
        with open(FLAGGED_CSV, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                existing_keys.add((r["file_id"], r["speech_id"], r["failure_type"]))
    with open(FLAGGED_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FLAGGED_FIELDNAMES)
        if write_header:
            writer.writeheader()
        for row in rows:
            key = (row.get("file_id", ""), row.get("speech_id", ""), row.get("failure_type", ""))
            if key not in existing_keys:
                existing_keys.add(key)
                writer.writerow({k: row.get(k, "") for k in FLAGGED_FIELDNAMES})


def run_qa(
    speeches: list[dict],
    session: dict,
    scanned_pages: list[int] | None = None,
) -> list[dict]:
    """
    Run structural checks on the speeches extracted from one session file.

    Parameters
    ----------
    speeches     : list of speech turn dicts from turn_segmenter
    session      : session metadata dict (from header_parser + preamble_detector)
    scanned_pages: list of page numbers that were skipped (scanned)

    Returns
    -------
    list of flag dicts appended to flagged.csv (also written to disk)
    """
    flags: list[dict] = []
    file_id = session.get("file_id", "")
    rule_version = session.get("rule_version", "")
    year = session.get("year_extracted")
    n_pages = session.get("n_pages_total", 0)

    def flag(speech_id, failure_type, severity, excerpt="", diagnosis=""):
        flags.append({
            "file_id": file_id,
            "speech_id": speech_id,
            "page_num": "",
            "year_extracted": year or "",
            "failure_type": failure_type,
            "severity": severity,
            "original_excerpt": excerpt[:500] if excerpt else "",
            "diagnosis": diagnosis,
            "rule_version": rule_version,
            "resolved": "false",
        })

    # ── Check: no date extracted ─────────────────────────────────────────────
    if not session.get("session_date"):
        flag("", "no_date_extracted", "blocking",
             diagnosis="session_date is null after header parsing")

    # ── Check: few speeches for a multi-page file ────────────────────────────
    n_speeches = len(speeches)
    if n_speeches < 3 and n_pages > 2:
        flag("", "few_speeches_extracted", "blocking",
             diagnosis=f"Only {n_speeches} speeches found in {n_pages}-page file")

    # ── Check: scanned pages present ─────────────────────────────────────────
    if scanned_pages:
        flag("", "scanned_pages_present", "info",
             diagnosis=f"Scanned pages: {scanned_pages}")

    # ── Per-speech checks ─────────────────────────────────────────────────────
    prev_speaker = None
    same_speaker_run = 0

    for sp in speeches:
        sid = sp.get("speech_id", "")
        wc = sp.get("word_count", 0)
        attribution = sp.get("attribution_raw", "")
        text_excerpt = (sp.get("speech_text") or "")[:200]

        if wc > 3000:
            flag(sid, "speech_too_long", "warning", text_excerpt,
                 f"word_count={wc}")

        # Skip short-speech check for role-label turns (Presidente, Secretaria, etc.)
        # — those are legitimately brief. Role turns have short attributions (≤3 words)
        # or start with a known role token.
        _ROLE_TOKENS = {"Presidente", "Presidenta", "Vicepresidente", "Vicepresidenta",
                        "Secretario", "Secretaria", "Subsecretario", "Subsecretaria",
                        "Presidencia"}
        first_word = attribution.split()[0] if attribution else ""
        is_role_turn = (len(attribution.split()) <= 3 or
                        first_word in _ROLE_TOKENS or
                        any(tok in attribution for tok in ("El Presidente", "La Presidenta",
                                                           "El Secretario", "La Secretaria")))
        if wc < 15 and not is_role_turn:
            flag(sid, "speech_too_short", "warning", text_excerpt,
                 f"word_count={wc}")

        # attribution_raw can be a full floor-grant narration (e.g. "La Presidencia concede el uso
        # de la palabra a la honorable Representante ...") so 100 chars is too short. Flag only
        # if it contains digits (corrupt text) or exceeds 300 chars (likely a segmentation error).
        if any(c.isdigit() for c in attribution) or len(attribution) > 300:
            flag(sid, "attribution_anomaly", "warning", attribution,
                 "Attribution raw string contains digits or is unusually long")

        # Consecutive same speaker (3+ without continuation)
        if attribution == prev_speaker and not sp.get("is_continuation"):
            same_speaker_run += 1
            if same_speaker_run >= 2:
                flag(sid, "consecutive_same_speaker", "info",
                     f"Attribution '{attribution}' appears {same_speaker_run + 1}x consecutively")
        else:
            same_speaker_run = 0
            prev_speaker = attribution

    _append_flag(flags)
    return flags
