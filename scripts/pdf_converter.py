"""
pdf_converter.py — Stage 1 of the Gaceta del Congreso pipeline.

Converts a single PDF to a list of clean, column-ordered page objects.
No LLM — pure pdfplumber logic.

Output per page:
    {
        "page_num": int,          # 1-indexed
        "text": str,              # clean extracted text
        "status": "ok" | "scanned" | "empty" | "error",
        "column_layout": "two_column" | "single_column"
    }

Scanned pages are also written to qa/scanned_pages.csv.
"""

import csv
import os
import re
import traceback
from pathlib import Path
from typing import Any

import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCANNED_PAGES_CSV = PROJECT_ROOT / "qa" / "scanned_pages.csv"

# ── canonical coordinate constants (calibrated for 609.4 × 963.8 pt pages) ──
# All coordinate thresholds are scaled proportionally to actual page size.
CANONICAL_W = 609.4
CANONICAL_H = 963.8

HEADER_TOP_P1 = 210       # strip y < this on page 1 (canonical scale)
HEADER_TOP_REST = 84      # strip y < this on pages 2+ (canonical scale)
MASTHEAD_SIZE_MIN = 40    # page-1 decorative letters: size >= 40 (absolute pt)

LEFT_X0, LEFT_X1 = 50, 300
RIGHT_X0, RIGHT_X1 = 310, 609
FULL_X0, FULL_X1 = 50, 609
RIGHT_WORD_THRESHOLD = 10  # fewer words in right zone → single-column


def _is_white(color: Any) -> bool:
    """Return True if the color represents white (invisible) text."""
    if color is None:
        return False
    if color == 1 or color == (1, 1, 1):
        return True
    if isinstance(color, (list, tuple)) and len(color) >= 3:
        return all(c > 0.9 for c in color[:3])
    if isinstance(color, float) and color > 0.9:
        return True
    return False


def _log_scanned_page(file_id: str, page_num: int, cid_word_pct: float) -> None:
    SCANNED_PAGES_CSV.parent.mkdir(parents=True, exist_ok=True)
    write_header = not SCANNED_PAGES_CSV.exists()
    with open(SCANNED_PAGES_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["file_id", "page_num", "cid_word_pct"])
        if write_header:
            writer.writeheader()
        writer.writerow({"file_id": file_id, "page_num": page_num, "cid_word_pct": round(cid_word_pct, 4)})


# Lines that are entirely bold get this prefix in the output text.
# The turn_segmenter uses it to detect turn boundaries.
BOLD_LINE_PREFIX = "§"

# CID decoding — applies chr(cid + 29) offset confirmed for the dominant 2007-2017
# font subset used in this corpus.  Known exceptions override the offset for
# accented characters that fall outside the +29 ASCII mapping.
_CID_RE = re.compile(r"\(cid:(\d+)\)")

# Accented characters whose +29 result lands in the C1 control range (128-159)
# and cannot be detected via isalpha().  Verified against calibration files.
_CID_CHAR_MAP: dict[int, str] = {
    105: "á",
    112: "é",
    116: "í",
    120: "ñ",
    121: "ó",
    126: "ú",
}

# Punctuation allowed from the +29 offset.  Chosen to cover attribution endings
# (colon), role-description commas, and name hyphens — while excluding the
# digit and symbol range that appears in gaceta_107's incompatible encoding.
_CID_ALLOWED_PUNCT = set(",.:-")


def _decode_cid_text(text: str) -> str:
    """Decode CID tokens using the +29 offset with known exceptions.

    Substitutes only when the decoded character is alphabetic, whitespace, or
    common attribution punctuation.  This rejects encodings like gaceta_107
    where the same CID values decode to digits or symbols under +29, leaving
    those tokens as (cid:N) for the segmenter to handle.
    """
    if "(cid:" not in text:
        return text

    def _replace(m: re.Match) -> str:
        n = int(m.group(1))
        if n in _CID_CHAR_MAP:
            return _CID_CHAR_MAP[n]
        ch = chr(n + 29)
        if ch.isalpha() or ch.isspace() or ch in _CID_ALLOWED_PUNCT:
            return ch
        return m.group(0)

    return _CID_RE.sub(_replace, text)


def _is_bold_word(word: dict) -> bool:
    return "bold" in (word.get("fontname") or "").lower()


def _group_into_lines(words: list[dict], y_tol: float = 3.0) -> list[list[dict]]:
    """Group word dicts into lines.

    Words are sorted by top then x0. A word joins the current line when its top
    is within y_tol of the FIRST word's top in that line (same semantics as
    pdfplumber's extract_text y_tolerance). Using the first word's top (not the
    running bottom) prevents cascading merges across consecutive lines.
    """
    if not words:
        return []
    sorted_w = sorted(words, key=lambda w: (float(w.get("top", 0)), float(w.get("x0", 0))))
    lines: list[list[dict]] = []
    cur: list[dict] = [sorted_w[0]]
    cur_line_top = float(sorted_w[0].get("top", 0))
    for w in sorted_w[1:]:
        top = float(w.get("top", 0))
        if top - cur_line_top <= y_tol:
            cur.append(w)
        else:
            lines.append(sorted(cur, key=lambda x: float(x.get("x0", 0))))
            cur = [w]
            cur_line_top = top
    lines.append(sorted(cur, key=lambda x: float(x.get("x0", 0))))
    return lines


def _words_to_text_with_bold(words: list[dict]) -> str:
    """
    Reconstruct page text from pdfplumber word dicts.
    Lines where every word is bold are prefixed with BOLD_LINE_PREFIX (§).
    """
    lines = _group_into_lines(words)
    text_lines: list[str] = []
    for line_words in lines:
        line_text = " ".join(_decode_cid_text(w.get("text", "")) for w in line_words)
        if line_text.strip() and all(_is_bold_word(w) for w in line_words):
            text_lines.append(BOLD_LINE_PREFIX + line_text)
        else:
            text_lines.append(line_text)
    return "\n".join(text_lines)


def _extract_column_text(page, x0: float, x1: float, top: float = 0) -> str:
    """Extract text from a vertical strip of the page, starting at top."""
    cropped = page.crop((x0, top, min(x1, page.width), page.height))
    return cropped.extract_text(x_tolerance=3, y_tolerance=3) or ""


def convert_pdf(pdf_path: str | Path, file_id: str | None = None) -> list[dict]:
    """
    Convert a PDF file to a list of page objects.

    Parameters
    ----------
    pdf_path : path to the PDF
    file_id  : identifier used in scanned_pages.csv; defaults to stem of pdf_path

    Returns
    -------
    list of page dicts (see module docstring)
    """
    pdf_path = Path(pdf_path)
    if file_id is None:
        file_id = pdf_path.stem

    results = []

    try:
        pdf = pdfplumber.open(pdf_path)
    except Exception as exc:
        return [{"page_num": 0, "text": "", "status": "error",
                 "column_layout": "single_column",
                 "error_detail": str(exc)}]

    with pdf:
        for raw_page in pdf.pages:
            page_num = raw_page.page_number  # 1-indexed
            is_first_page = (page_num == 1)

            try:
                result = _process_page(raw_page, page_num, is_first_page, file_id)
            except Exception as exc:
                result = {
                    "page_num": page_num,
                    "text": "",
                    "status": "error",
                    "column_layout": "single_column",
                    "error_detail": traceback.format_exc(limit=3),
                }

            results.append(result)

    return results


def _process_page(raw_page, page_num: int, is_first_page: bool, file_id: str) -> dict:
    # Scale canonical coordinate thresholds to actual page dimensions.
    pw = raw_page.width or CANONICAL_W
    ph = raw_page.height or CANONICAL_H
    x_scale = pw / CANONICAL_W
    y_scale = ph / CANONICAL_H

    header_top_p1 = HEADER_TOP_P1 * y_scale
    header_top_rest = HEADER_TOP_REST * y_scale
    left_x0 = LEFT_X0 * x_scale
    left_x1 = LEFT_X1 * x_scale
    right_x0 = RIGHT_X0 * x_scale
    right_x1 = min(RIGHT_X1 * x_scale, pw)
    full_x0 = FULL_X0 * x_scale
    full_x1 = min(FULL_X1 * x_scale, pw)

    # ── step 1: get all words with metadata ─────────────────────────────────
    all_words = raw_page.extract_words(
        extra_attrs=["non_stroking_color", "size", "fontname"],
        use_text_flow=False,
    )

    # ── step 1: white text filter ────────────────────────────────────────────
    words = [w for w in all_words if not _is_white(w.get("non_stroking_color"))]

    # ── step 2: scan detection ───────────────────────────────────────────────
    if words:
        cid_count = sum(1 for w in words if "(cid:" in w.get("text", ""))
        cid_pct = cid_count / len(words)
    else:
        cid_pct = 0.0

    if cid_pct > 0.30:
        _log_scanned_page(file_id, page_num, cid_pct)
        return {"page_num": page_num, "text": "", "status": "scanned",
                "column_layout": "single_column", "cid_word_pct": round(cid_pct, 4)}

    # ── step 3: masthead filter (page 1 only) ────────────────────────────────
    if is_first_page:
        words = [w for w in words if float(w.get("size", 0) or 0) < MASTHEAD_SIZE_MIN]

    # ── step 4: header zone strip ────────────────────────────────────────────
    header_top = header_top_p1 if is_first_page else header_top_rest
    words = [w for w in words if float(w.get("top", 0) or 0) >= header_top]

    if not words:
        return {"page_num": page_num, "text": "", "status": "empty",
                "column_layout": "single_column"}

    # ── step 5: footer strip — remove lines containing IMPRENTA NACIONAL ────
    # Group words into lines by top coordinate (±3pt tolerance), then drop lines.
    from collections import defaultdict
    line_buckets: dict[int, list[dict]] = defaultdict(list)
    for w in words:
        bucket = round(float(w.get("top", 0)) / 3) * 3
        line_buckets[bucket].append(w)

    clean_words = []
    for bucket_top in sorted(line_buckets):
        line_words = line_buckets[bucket_top]
        line_text = " ".join(w.get("text", "") for w in line_words)
        if "IMPRENTA NACIONAL" in line_text:
            continue
        clean_words.extend(line_words)

    words = clean_words

    if not words:
        return {"page_num": page_num, "text": "", "status": "empty",
                "column_layout": "single_column"}

    # ── step 6: column detection ─────────────────────────────────────────────
    right_zone_words = [w for w in words if right_x0 <= float(w.get("x0", 0)) <= right_x1]
    is_two_column = len(right_zone_words) >= RIGHT_WORD_THRESHOLD

    column_layout = "two_column" if is_two_column else "single_column"

    # ── step 7: column-ordered extraction ────────────────────────────────────
    if is_two_column:
        left_words  = [w for w in words if left_x0  <= float(w.get("x0", 0)) <= left_x1]
        right_words = [w for w in words if right_x0 <= float(w.get("x0", 0)) <= right_x1]
        left_text  = _words_to_text_with_bold(left_words)
        right_text = _words_to_text_with_bold(right_words)
        text = (left_text.strip() + "\n" + right_text.strip()).strip()
    else:
        full_words = [w for w in words if full_x0 <= float(w.get("x0", 0)) <= full_x1]
        text = _words_to_text_with_bold(full_words).strip()

    if not text:
        return {"page_num": page_num, "text": "", "status": "empty",
                "column_layout": column_layout}

    return {"page_num": page_num, "text": text, "status": "ok",
            "column_layout": column_layout}


def _strip_imprenta(text: str) -> str:
    lines = text.splitlines()
    return "\n".join(ln for ln in lines if "IMPRENTA NACIONAL" not in ln)


# ── CLI entry point ──────────────────────────────────────────────────────────

def main():
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Convert PDFs to clean page-text JSON.")
    parser.add_argument("pdf_paths", nargs="+", help="PDF file(s) to convert")
    parser.add_argument("--output-dir", default=None,
                        help="Directory to write per-file JSON output. "
                             "Defaults to stdout if omitted.")
    parser.add_argument("--summary", action="store_true",
                        help="Print a one-line summary per file instead of full JSON.")
    args = parser.parse_args()

    for pdf_path in args.pdf_paths:
        pdf_path = Path(pdf_path)
        file_id = pdf_path.stem
        pages = convert_pdf(pdf_path, file_id=file_id)

        if args.summary:
            n_ok = sum(1 for p in pages if p["status"] == "ok")
            n_scanned = sum(1 for p in pages if p["status"] == "scanned")
            n_empty = sum(1 for p in pages if p["status"] == "empty")
            n_error = sum(1 for p in pages if p["status"] == "error")
            two_col = sum(1 for p in pages if p.get("column_layout") == "two_column")
            print(f"{file_id:40s}  pages={len(pages):3d}  "
                  f"ok={n_ok:3d}  scanned={n_scanned:2d}  "
                  f"empty={n_empty:2d}  error={n_error:2d}  "
                  f"two_col={two_col:3d}")
        elif args.output_dir:
            out_dir = Path(args.output_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"{file_id}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(pages, f, ensure_ascii=False, indent=2)
            print(f"Wrote {out_file}")
        else:
            print(json.dumps({file_id: pages}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
