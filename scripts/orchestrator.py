"""
orchestrator.py — Main pipeline entry point.

Runs the full extraction pipeline on a directory of pre-converted JSON files
(output of pdf_converter) or directly on PDF files.

Usage (calibration run):
    py -3 scripts/orchestrator.py --mode calibration

Usage (full run):
    py -3 scripts/orchestrator.py --mode full

Outputs:
    output/speeches.csv
    output/sessions.csv
    output/extraction_log.csv
    qa/flagged.csv
    qa/scanned_pages.csv   (written by pdf_converter during conversion)
"""

import argparse
import csv
import json
import os
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from preamble_detector import detect_preamble
from header_parser import parse_header
from turn_segmenter import segment_turns, RULE_VERSION
from qa_agent import run_qa, _append_flag

# ── Output paths ──────────────────────────────────────────────────────────────

OUTPUT_DIR = PROJECT_ROOT / "output"
SPEECHES_CSV = OUTPUT_DIR / "speeches.csv"
SESSIONS_CSV = OUTPUT_DIR / "sessions.csv"
EXTRACTION_LOG = OUTPUT_DIR / "extraction_log.csv"

SPEECHES_FIELDS = [
    "speech_id", "session_id", "file_id", "gaceta_number", "publication_date",
    "session_date", "chamber", "committee", "attribution_raw",
    "speech_text", "word_count", "position_in_session",
    "is_continuation", "record_type", "rule_version", "qa_flag", "date_source",
    # TODO: speaker_type, role_label, speaker_name — deferred to classification stage
]

SESSIONS_FIELDS = [
    "session_id", "file_id", "gaceta_number", "publication_date", "session_date",
    "year_extracted", "chamber", "committee", "document_type", "transcript_start_page",
    "n_pages_total", "n_pages_scanned", "n_speeches", "n_flagged",
    "processing_status", "rule_version", "processing_timestamp", "header_confidence",
    "extraction_status",
    # TODO: n_uncertain — deferred to classification stage
]

LOG_FIELDS = [
    "file_id", "status", "rule_version", "n_speeches", "n_flagged",
    "error_detail", "processing_timestamp",
]


def _open_csv(path: Path, fieldnames: list[str]) -> tuple:
    """Open a CSV in append mode; write header if new file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    fh = open(path, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
    if write_header:
        writer.writeheader()
    return fh, writer


def _log_entry(log_writer, file_id, status, n_speeches=0, n_flagged=0, error=""):
    log_writer.writerow({
        "file_id": file_id,
        "status": status,
        "rule_version": RULE_VERSION,
        "n_speeches": n_speeches,
        "n_flagged": n_flagged,
        "error_detail": error[:500] if error else "",
        "processing_timestamp": datetime.now(timezone.utc).isoformat(),
    })


def _load_scanned_pages(file_id: str) -> list[int]:
    """Return list of scanned page numbers for this file_id."""
    csv_path = PROJECT_ROOT / "qa" / "scanned_pages.csv"
    if not csv_path.exists():
        return []
    scanned = []
    with open(csv_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("file_id") == file_id:
                try:
                    scanned.append(int(row["page_num"]))
                except (ValueError, KeyError):
                    pass
    return scanned


def process_file(
    file_id: str,
    pages: list[dict],
    speeches_writer,
    sessions_writer,
    log_writer,
) -> dict:
    """
    Run the full pipeline on one pre-converted file.

    Returns a summary dict for the manifest.
    """
    ts = datetime.now(timezone.utc).isoformat()

    try:
        # Stage 2: preamble detection
        preamble_info = detect_preamble(pages)
        transcript_start = preamble_info["transcript_start_page"]

        # Stage 3: header parsing
        preamble_pages = [
            p for p in pages
            if p.get("status") == "ok"
            and (transcript_start is None or p["page_num"] < transcript_start)
        ]
        # Always include page 1 in preamble text
        page1 = next((p for p in pages if p["page_num"] == 1 and p.get("status") == "ok"), None)
        preamble_texts = []
        if page1:
            preamble_texts.append(page1.get("text", ""))
        for p in preamble_pages:
            if p["page_num"] != 1:
                preamble_texts.append(p.get("text", ""))
        preamble_text = "\n".join(preamble_texts)

        meta = parse_header(preamble_text, file_id=file_id)

        # Stage 4: turn segmentation
        if transcript_start is not None:
            transcript_pages = [
                p for p in pages
                if p.get("status") == "ok" and p["page_num"] >= transcript_start
            ]
        else:
            transcript_pages = [p for p in pages if p.get("status") == "ok"]

        turns = segment_turns(transcript_pages, file_id=file_id, session_meta=meta)

        # Build flagged set for speech records
        n_pages_total = len(pages)
        n_pages_scanned = sum(1 for p in pages if p.get("status") == "scanned")
        # Extract year
        session_date = meta.get("session_date")
        year_extracted = None
        if session_date:
            try:
                year_extracted = int(session_date[:4])
            except (ValueError, TypeError):
                pass

        # Stage 5: QA
        session_summary = {
            "file_id": file_id,
            "rule_version": RULE_VERSION,
            "session_date": session_date,
            "year_extracted": year_extracted,
            "n_pages_total": n_pages_total,
        }
        scanned_pages = _load_scanned_pages(file_id)
        flags = run_qa(turns, session_summary, scanned_pages=scanned_pages or None)
        n_flagged = len(flags)

        flagged_speech_ids = {f["speech_id"] for f in flags if f.get("speech_id")}

        for t in turns:
            row = {
                "speech_id": t["speech_id"],
                "file_id": file_id,
                "year_id": "",
                "gaceta_number": meta.get("gaceta_number", ""),
                "publication_date": meta.get("publication_date", ""),
                "session_date": session_date or "",
                "chamber": meta.get("chamber", ""),
                "committee": meta.get("committee", ""),
                "attribution_raw": t["attribution_raw"],
                "speech_text": t["speech_text"],
                "word_count": t["word_count"],
                "position_in_session": t["position_in_session"],
                "is_continuation": t.get("is_continuation", False),
                "record_type": t.get("record_type", "speech"),
                "rule_version": RULE_VERSION,
                "qa_flag": t["speech_id"] in flagged_speech_ids,
                "date_source": meta.get("date_source", "header_extracted"),
            }
            speeches_writer.writerow(row)

        # Lowest confidence across header fields
        confidences = [
            meta.get("publication_date_confidence"),
            meta.get("session_date_confidence"),
            meta.get("committee_confidence"),
            meta.get("presiding_officer_confidence"),
        ]
        conf_order = {"low": 0, "medium": 1, "high": 2, None: 3}
        header_confidence = min(
            (c for c in confidences if c),
            key=lambda x: conf_order.get(x, 3),
            default="unknown",
        )

        # Write session record
        sessions_writer.writerow({
            "file_id": file_id,
            "year_id": "",
            "gaceta_number": meta.get("gaceta_number", ""),
            "publication_date": meta.get("publication_date", ""),
            "session_date": session_date or "",
            "year_extracted": year_extracted or "",
            "chamber": meta.get("chamber", ""),
            "committee": meta.get("committee", ""),
            "document_type": meta.get("document_type", ""),
            "transcript_start_page": transcript_start or "",
            "n_pages_total": n_pages_total,
            "n_pages_scanned": n_pages_scanned,
            "n_speeches": len(turns),
            "n_flagged": n_flagged,
            "processing_status": "done",
            "rule_version": RULE_VERSION,
            "processing_timestamp": ts,
            "header_confidence": header_confidence,
            "extraction_status": meta.get("extraction_status", ""),
        })

        _log_entry(log_writer, file_id, "done", len(turns), n_flagged)

        return {
            "status": "done",
            "n_speeches": len(turns),
            "n_flagged": n_flagged,
            "session_date": session_date,
            "year_extracted": year_extracted,
        }

    except Exception:
        error = traceback.format_exc(limit=5)
        _log_entry(log_writer, file_id, "failed", error=error)
        print(f"  ERROR in {file_id}: {error.splitlines()[-1]}", flush=True)
        return {"status": "failed", "error": error}


def _assign_year_ids():
    """
    Post-process speeches.csv and sessions.csv to assign monotonically increasing
    year-based IDs. Files are sorted by (session_date, gaceta_number) within each
    year and assigned IDs like '2022_0001'. speech_id is rebuilt accordingly.
    """
    import re as _re

    def _load_csv(path):
        if not path.exists():
            return [], []
        csv.field_size_limit(10 ** 7)
        with open(path, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = reader.fieldnames or []
        return rows, fieldnames

    sessions, sess_fields = _load_csv(SESSIONS_CSV)
    speeches, sp_fields = _load_csv(SPEECHES_CSV)

    if not sessions:
        return

    def _sort_key(row):
        date = row.get("session_date") or "9999-12-31"
        gaceta = row.get("gaceta_number") or "99999"
        try:
            g = int(_re.sub(r"[^\d]", "", gaceta))
        except ValueError:
            g = 99999
        return (date, g)

    sessions_sorted = sorted(sessions, key=_sort_key)

    year_counters: dict[str, int] = {}
    file_id_to_year_id: dict[str, str] = {}

    for row in sessions_sorted:
        session_date = row.get("session_date") or ""
        year = session_date[:4] if len(session_date) >= 4 else "0000"
        year_counters[year] = year_counters.get(year, 0) + 1
        year_id = f"{year}_{year_counters[year]:04d}"
        file_id_to_year_id[row["file_id"]] = year_id
        row["year_id"] = year_id

    for row in speeches:
        fid = row.get("file_id", "")
        yid = file_id_to_year_id.get(fid, "")
        row["year_id"] = yid
        pos = row.get("position_in_session", "")
        try:
            pos_int = int(pos)
            row["speech_id"] = f"{yid}_{pos_int:03d}"
        except (ValueError, TypeError):
            pass

    def _write_csv(path, rows, fieldnames):
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

    _write_csv(SESSIONS_CSV, sessions_sorted, sess_fields)
    _write_csv(SPEECHES_CSV, speeches, sp_fields)


def run_calibration():
    """Process all pre-converted calibration JSONs."""
    calib_dir = OUTPUT_DIR / "pdf_converter_calibration"
    json_files = sorted(calib_dir.glob("*.json"))

    if not json_files:
        print("No calibration JSONs found in", calib_dir)
        sys.exit(1)

    print(f"Found {len(json_files)} calibration files.")
    print(f"Rule version: {RULE_VERSION}")
    print()

    for path in (SPEECHES_CSV, SESSIONS_CSV, EXTRACTION_LOG,
                 PROJECT_ROOT / "qa" / "flagged.csv"):
        if path.exists():
            path.unlink()

    speeches_fh, speeches_w = _open_csv(SPEECHES_CSV, SPEECHES_FIELDS)
    sessions_fh, sessions_w = _open_csv(SESSIONS_CSV, SESSIONS_FIELDS)
    log_fh, log_w = _open_csv(EXTRACTION_LOG, LOG_FIELDS)

    try:
        for jf in json_files:
            file_id = jf.stem
            print(f"  Processing {file_id} ...", end=" ", flush=True)

            with open(jf, encoding="utf-8") as f:
                pages = json.load(f)

            if not pages:
                print("SKIP (empty JSON)")
                _log_entry(log_w, file_id, "skipped_empty")
                continue

            all_scanned = all(p.get("status") == "scanned" for p in pages)
            if all_scanned:
                print("SKIP (all pages scanned)")
                _log_entry(log_w, file_id, "skipped_all_scanned")
                continue

            result = process_file(file_id, pages, speeches_w, sessions_w, log_w)
            speeches_fh.flush()
            sessions_fh.flush()
            log_fh.flush()

            status = result.get("status", "?")
            n = result.get("n_speeches", 0)
            nf = result.get("n_flagged", 0)
            print(f"{status}  speeches={n}  flagged={nf}")

    finally:
        speeches_fh.close()
        sessions_fh.close()
        log_fh.close()

    _assign_year_ids()

    print()
    print(f"Done. Output written to:")
    print(f"  {SPEECHES_CSV}")
    print(f"  {SESSIONS_CSV}")
    print(f"  {EXTRACTION_LOG}")


# ── Manifest helpers ──────────────────────────────────────────────────────────

STATE_DIR   = PROJECT_ROOT / "state"
MANIFEST    = STATE_DIR / "manifest.json"
INPUT_DIR   = PROJECT_ROOT / "input"

BATCH_SIZE  = 50
MAX_WORKERS = 3


def _build_manifest() -> dict:
    pdfs = sorted(INPUT_DIR.glob("*.pdf"))
    files = {p.stem: {"status": "pending", "session_id": i + 1}
             for i, p in enumerate(pdfs)}
    manifest = {
        "total_files": len(files),
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "rule_version_current": RULE_VERSION,
        "files": files,
    }
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def _load_manifest() -> dict:
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def _save_manifest(manifest: dict) -> None:
    manifest["last_updated"] = datetime.now(timezone.utc).isoformat()
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


# ── Per-file worker (top-level so multiprocessing can pickle it) ──────────────

def _worker_process_pdf(args: tuple) -> dict:
    """
    Run the full pipeline on one PDF. Returns all results as plain dicts —
    no file I/O except for scanned_pages.csv (written by pdf_converter).

    Returns dict with keys:
        file_id, session_id, status, speech_rows, session_row, log_row, flag_rows, error
    """
    file_id, pdf_path_str, session_id = args
    pdf_path = Path(pdf_path_str)
    ts = datetime.now(timezone.utc).isoformat()

    empty = {"file_id": file_id, "session_id": session_id, "speech_rows": [],
             "session_row": {}, "log_row": {}, "flag_rows": []}

    try:
        # Stage 1: PDF conversion
        sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
        from pdf_converter import convert_pdf
        pages = convert_pdf(pdf_path, file_id=file_id)

        if not pages:
            row = {"file_id": file_id, "status": "failed", "rule_version": RULE_VERSION,
                   "n_speeches": 0, "n_flagged": 0,
                   "error_detail": "convert_pdf returned empty list",
                   "processing_timestamp": ts}
            return {**empty, "status": "failed", "log_row": row}

        if all(p.get("status") in ("scanned", "empty", "error") for p in pages):
            row = {"file_id": file_id, "status": "skipped_all_scanned",
                   "rule_version": RULE_VERSION, "n_speeches": 0, "n_flagged": 0,
                   "error_detail": "", "processing_timestamp": ts}
            return {**empty, "status": "skipped_all_scanned", "log_row": row}

        # Stage 2: preamble detection
        preamble_info = detect_preamble(pages)
        transcript_start = preamble_info["transcript_start_page"]

        # Stage 3: header parsing
        page1 = next((p for p in pages if p["page_num"] == 1 and p.get("status") == "ok"), None)
        preamble_pages = [p for p in pages
                          if p.get("status") == "ok"
                          and (transcript_start is None or p["page_num"] < transcript_start)]
        preamble_texts = []
        if page1:
            preamble_texts.append(page1.get("text", ""))
        for p in preamble_pages:
            if p["page_num"] != 1:
                preamble_texts.append(p.get("text", ""))
        meta = parse_header("\n".join(preamble_texts), file_id=file_id)

        # Stage 4: segmentation
        transcript_pages = [p for p in pages
                            if p.get("status") == "ok"
                            and (transcript_start is None or p["page_num"] >= transcript_start)]
        turns = segment_turns(transcript_pages, file_id=file_id, session_meta=meta)

        # Metadata
        session_date = meta.get("session_date")
        year_extracted = None
        if session_date:
            try:
                year_extracted = int(session_date[:4])
            except (ValueError, TypeError):
                pass

        n_pages_total   = len(pages)
        n_pages_scanned = sum(1 for p in pages if p.get("status") == "scanned")

        # Stage 5: QA (compute only — main process writes flagged.csv)
        session_summary = {
            "file_id": file_id,
            "rule_version": RULE_VERSION,
            "session_date": session_date,
            "year_extracted": year_extracted,
            "n_pages_total": n_pages_total,
        }
        scanned_page_nums = [p["page_num"] for p in pages if p.get("status") == "scanned"]
        flag_rows = _compute_qa_flags(turns, session_summary, scanned_page_nums)
        n_flagged = len(flag_rows)
        flagged_ids = {f["speech_id"] for f in flag_rows if f.get("speech_id")}

        # Build speech rows
        speech_rows = []
        for t in turns:
            speech_rows.append({
                "speech_id": f"{session_id}_{t['position_in_session']:03d}",
                "session_id": session_id,
                "file_id": file_id,
                "gaceta_number": meta.get("gaceta_number", ""),
                "publication_date": meta.get("publication_date", ""),
                "session_date": session_date or "",
                "chamber": meta.get("chamber", ""),
                "committee": meta.get("committee", ""),
                "attribution_raw": t["attribution_raw"],
                "speech_text": t["speech_text"],
                "word_count": t["word_count"],
                "position_in_session": t["position_in_session"],
                "is_continuation": t.get("is_continuation", False),
                "record_type": t.get("record_type", "speech"),
                "rule_version": RULE_VERSION,
                "qa_flag": t["speech_id"] in flagged_ids,
                "date_source": meta.get("date_source", "header_extracted"),
            })

        # Build session row
        confidences = [meta.get("publication_date_confidence"),
                       meta.get("session_date_confidence"),
                       meta.get("committee_confidence"),
                       meta.get("presiding_officer_confidence")]
        conf_order = {"low": 0, "medium": 1, "high": 2, None: 3}
        header_confidence = min(
            (c for c in confidences if c),
            key=lambda x: conf_order.get(x, 3),
            default="unknown",
        )
        session_row = {
            "session_id": session_id,
            "file_id": file_id,
            "gaceta_number": meta.get("gaceta_number", ""),
            "publication_date": meta.get("publication_date", ""),
            "session_date": session_date or "",
            "year_extracted": year_extracted or "",
            "chamber": meta.get("chamber", ""),
            "committee": meta.get("committee", ""),
            "document_type": meta.get("document_type", ""),
            "transcript_start_page": transcript_start or "",
            "n_pages_total": n_pages_total,
            "n_pages_scanned": n_pages_scanned,
            "n_speeches": len(turns),
            "n_flagged": n_flagged,
            "processing_status": "done",
            "rule_version": RULE_VERSION,
            "processing_timestamp": ts,
            "header_confidence": header_confidence,
            "extraction_status": meta.get("extraction_status", ""),
        }

        log_row = {
            "file_id": file_id,
            "status": "done",
            "rule_version": RULE_VERSION,
            "n_speeches": len(turns),
            "n_flagged": n_flagged,
            "error_detail": "",
            "processing_timestamp": ts,
        }

        return {
            "file_id": file_id,
            "status": "done",
            "speech_rows": speech_rows,
            "session_row": session_row,
            "log_row": log_row,
            "flag_rows": flag_rows,
        }

    except Exception:
        error = traceback.format_exc(limit=5)
        log_row = {
            "file_id": file_id,
            "status": "failed",
            "rule_version": RULE_VERSION,
            "n_speeches": 0,
            "n_flagged": 0,
            "error_detail": error[:500],
            "processing_timestamp": ts,
        }
        return {**empty, "status": "failed", "log_row": log_row, "error": error}


def _compute_qa_flags(speeches, session, scanned_pages):
    """QA checks without writing to disk — mirrors run_qa logic."""
    flags = []
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

    if not session.get("session_date"):
        flag("", "no_date_extracted", "blocking",
             diagnosis="session_date is null after header parsing")

    n_speeches = len(speeches)
    if n_speeches < 3 and n_pages > 2:
        flag("", "few_speeches_extracted", "blocking",
             diagnosis=f"Only {n_speeches} speeches found in {n_pages}-page file")

    if scanned_pages:
        flag("", "scanned_pages_present", "info",
             diagnosis=f"Scanned pages: {scanned_pages}")

    _ROLE_TOKENS = {"Presidente", "Presidenta", "Vicepresidente", "Vicepresidenta",
                    "Secretario", "Secretaria", "Subsecretario", "Subsecretaria", "Presidencia"}
    prev_speaker = None
    same_speaker_run = 0

    for sp in speeches:
        sid = sp.get("speech_id", "")
        wc = sp.get("word_count", 0)
        attribution = sp.get("attribution_raw", "")
        text_excerpt = (sp.get("speech_text") or "")[:200]

        if wc > 3000:
            flag(sid, "speech_too_long", "warning", text_excerpt, f"word_count={wc}")

        first_word = attribution.split()[0] if attribution else ""
        is_role_turn = (len(attribution.split()) <= 3 or first_word in _ROLE_TOKENS or
                        any(tok in attribution for tok in
                            ("El Presidente", "La Presidenta", "El Secretario", "La Secretaria")))
        if wc < 15 and not is_role_turn:
            flag(sid, "speech_too_short", "warning", text_excerpt, f"word_count={wc}")

        if any(c.isdigit() for c in attribution) or len(attribution) > 300:
            flag(sid, "attribution_anomaly", "warning", attribution,
                 "Attribution raw string contains digits or is unusually long")

        if attribution == prev_speaker and not sp.get("is_continuation"):
            same_speaker_run += 1
            if same_speaker_run >= 2:
                flag(sid, "consecutive_same_speaker", "info",
                     f"Attribution '{attribution}' appears {same_speaker_run + 1}x consecutively")
        else:
            same_speaker_run = 0
            prev_speaker = attribution

    return flags


# ── Full run ──────────────────────────────────────────────────────────────────

def run_full(workers: int = MAX_WORKERS, batch_size: int = BATCH_SIZE):
    # Build or load manifest
    if MANIFEST.exists():
        manifest = _load_manifest()
        # Reset interrupted files
        reset = 0
        for fid, info in manifest["files"].items():
            if info["status"] == "processing":
                manifest["files"][fid]["status"] = "pending"
                reset += 1
        if reset:
            print(f"Reset {reset} interrupted files to pending.")
    else:
        print("Building manifest from input directory...")
        manifest = _build_manifest()
        print(f"  {manifest['total_files']} files registered.")

    pending = [fid for fid, info in manifest["files"].items()
               if info["status"] == "pending"]

    if not pending:
        print("Nothing to process — all files are done or failed.")
        return

    total_pending = len(pending)
    total_files   = manifest["total_files"]
    already_done  = total_files - total_pending
    print(f"Rule version : {RULE_VERSION}")
    print(f"Total files  : {total_files}")
    print(f"Already done : {already_done}")
    print(f"To process   : {total_pending}  (batch_size={batch_size}, workers={workers})")
    print()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    speeches_fh, speeches_w = _open_csv(SPEECHES_CSV, SPEECHES_FIELDS)
    sessions_fh, sessions_w = _open_csv(SESSIONS_CSV, SESSIONS_FIELDS)
    log_fh,      log_w      = _open_csv(EXTRACTION_LOG, LOG_FIELDS)

    files_done   = already_done
    files_failed = sum(1 for info in manifest["files"].values() if info["status"] == "failed")
    batch_num    = 0
    all_flags    = []

    try:
        for batch_start in range(0, total_pending, batch_size):
            batch = pending[batch_start:batch_start + batch_size]
            batch_num += 1

            # Mark batch as processing and checkpoint
            for fid in batch:
                manifest["files"][fid]["status"] = "processing"
            _save_manifest(manifest)

            args = [(fid, str(INPUT_DIR / f"{fid}.pdf"),
                     manifest["files"][fid]["session_id"]) for fid in batch]

            print(f"Batch {batch_num}  ({batch_start + 1}–{batch_start + len(batch)}"
                  f" of {total_pending})  ...", flush=True)

            with ProcessPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(_worker_process_pdf, a): a[0] for a in args}
                for fut in as_completed(futures):
                    fid = futures[fut]
                    try:
                        result = fut.result()
                    except Exception as exc:
                        result = {
                            "file_id": fid, "status": "failed",
                            "speech_rows": [], "session_row": {}, "flag_rows": [],
                            "log_row": {
                                "file_id": fid, "status": "failed",
                                "rule_version": RULE_VERSION, "n_speeches": 0, "n_flagged": 0,
                                "error_detail": str(exc)[:500],
                                "processing_timestamp": datetime.now(timezone.utc).isoformat(),
                            },
                        }

                    status = result["status"]
                    manifest["files"][fid]["status"] = status
                    manifest["files"][fid]["rule_version"] = RULE_VERSION
                    manifest["files"][fid]["processing_timestamp"] = \
                        result.get("log_row", {}).get("processing_timestamp", "")

                    if status == "done":
                        files_done += 1
                        sr = result.get("session_row", {})
                        manifest["files"][fid].update({
                            "n_speeches": sr.get("n_speeches", 0),
                            "n_flagged":  sr.get("n_flagged", 0),
                            "session_date": sr.get("session_date", ""),
                            "year_extracted": sr.get("year_extracted", ""),
                            "document_type": sr.get("document_type", ""),
                        })
                        for row in result["speech_rows"]:
                            speeches_w.writerow(row)
                        sessions_w.writerow(sr)
                        all_flags.extend(result.get("flag_rows", []))
                    elif status == "failed":
                        files_failed += 1

                    log_row = result.get("log_row")
                    if log_row:
                        log_w.writerow(log_row)

            # Flush CSVs and checkpoint manifest after each batch
            speeches_fh.flush()
            sessions_fh.flush()
            log_fh.flush()
            _save_manifest(manifest)

            # Write accumulated flags every batch
            if all_flags:
                _append_flag(all_flags)
                all_flags = []

            done_pct = 100 * files_done / total_files
            print(f"  done={files_done}  failed={files_failed}  "
                  f"({done_pct:.1f}% of corpus)", flush=True)

    finally:
        speeches_fh.close()
        sessions_fh.close()
        log_fh.close()
        if all_flags:
            _append_flag(all_flags)
        _save_manifest(manifest)

    print()
    print(f"Done. Output:")
    print(f"  {SPEECHES_CSV}")
    print(f"  {SESSIONS_CSV}")
    print(f"  {EXTRACTION_LOG}")


def main():
    parser = argparse.ArgumentParser(description="Gaceta del Congreso pipeline orchestrator")
    parser.add_argument(
        "--mode", choices=["calibration", "full"], default="calibration",
        help="'calibration' processes pre-converted JSONs; 'full' processes raw PDFs",
    )
    parser.add_argument("--workers",    type=int, default=MAX_WORKERS)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    args = parser.parse_args()

    if args.mode == "calibration":
        run_calibration()
    else:
        run_full(workers=args.workers, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
