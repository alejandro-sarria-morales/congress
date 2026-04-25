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
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from preamble_detector import detect_preamble
from header_parser import parse_header
from turn_segmenter import segment_turns, RULE_VERSION
from qa_agent import run_qa

# ── Output paths ──────────────────────────────────────────────────────────────

OUTPUT_DIR = PROJECT_ROOT / "output"
SPEECHES_CSV = OUTPUT_DIR / "speeches.csv"
SESSIONS_CSV = OUTPUT_DIR / "sessions.csv"
EXTRACTION_LOG = OUTPUT_DIR / "extraction_log.csv"

SPEECHES_FIELDS = [
    "speech_id", "file_id", "year_id", "gaceta_number", "publication_date",
    "session_date", "chamber", "committee", "attribution_raw",
    "speech_text", "word_count", "position_in_session",
    "is_continuation", "record_type", "rule_version", "qa_flag", "date_source",
    # TODO: speaker_type, role_label, speaker_name — deferred to classification stage
]

SESSIONS_FIELDS = [
    "file_id", "year_id", "gaceta_number", "publication_date", "session_date",
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


def main():
    parser = argparse.ArgumentParser(description="Gaceta del Congreso pipeline orchestrator")
    parser.add_argument(
        "--mode", choices=["calibration", "full"], default="calibration",
        help="'calibration' processes pre-converted JSONs; 'full' processes raw PDFs",
    )
    args = parser.parse_args()

    if args.mode == "calibration":
        run_calibration()
    else:
        print("Full mode not yet implemented. Run pdf_converter.py first, then use --mode calibration.")
        sys.exit(1)


if __name__ == "__main__":
    main()
