"""
calibration_checker.py — Post-run verification for calibration rounds.

Checks each issue raised in calibration/feedback.md against the current
output/speeches.csv and output/sessions.csv, and prints a structured
PASS / WARN / FAIL report.

Usage:
    py -3 scripts/calibration_checker.py
"""

import csv
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SPEECHES_CSV = PROJECT_ROOT / "output" / "speeches.csv"
SESSIONS_CSV = PROJECT_ROOT / "output" / "sessions.csv"

csv.field_size_limit(10 ** 7)

PASS = "PASS"
WARN = "WARN"
FAIL = "FAIL"
INFO = "INFO"


def load_csv(path):
    if not path.exists():
        print(f"ERROR: {path} not found")
        sys.exit(1)
    with open(path, encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    return rows


def by_file(rows, file_id):
    return [r for r in rows if r["file_id"] == file_id]


def report(status, label, detail=""):
    sym = {"PASS": "+", "WARN": "?", "FAIL": "!", "INFO": "i"}[status]
    line = f"  [{sym}] {label}"
    if detail:
        line += f"\n      {detail}"
    print(line)


# ── Session-closing patterns ──────────────────────────────────────────────────
_CLOSE_RE = re.compile(
    r"(?:Se levanta|Se da por terminada?|Se termina|Con esto se da|Se da fin a)"
    r"\s+la\s+sesi[oó]n",
    re.IGNORECASE,
)

# ── Running-header pattern in body text ───────────────────────────────────────
_HEADER_RE = re.compile(
    r"(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|"
    r"octubre|noviembre|diciembre)\s+de\s+\d{4}\s+(?:Gaceta|GACETA)",
    re.IGNORECASE,
)

# ── Procedural dash pattern ───────────────────────────────────────────────────
_DASH_RE = re.compile(r"(?m)^-\s+[A-Z]")

# ── Trailing-verb pattern in speaker_raw ─────────────────────────────────────
_TRAILING_VERB_RE = re.compile(
    r"[,;.]\s*(?:quien\s+)?(?:expresa|indica|manifiesta|afirma|se[ñn]ala|"
    r"interviene|da apertura|hace uso de la palabra|tiene la palabra|"
    r"inicia|contesta|responde|solicita|presenta|propone)\b",
    re.IGNORECASE,
)


def main():
    speeches = load_csv(SPEECHES_CSV)
    sessions = load_csv(SESSIONS_CSV)

    fields = list(speeches[0].keys()) if speeches else []
    sess_fields = list(sessions[0].keys()) if sessions else []

    print()
    print("=" * 60)
    print("CALIBRATION CHECK REPORT")
    print("=" * 60)

    # ── FIX 1: Schema — removed fields ───────────────────────────────────────
    print("\n[Fix 1] Schema: deprecated fields removed")
    for bad_field in ("act_number", "presiding_officer", "legislature"):
        if bad_field in fields or bad_field in sess_fields:
            report(FAIL, f"Field '{bad_field}' still present in CSV headers")
        else:
            report(PASS, f"Field '{bad_field}' absent")

    # ── FIX 2: Schema — year_id present ──────────────────────────────────────
    print("\n[Fix 2] Schema: year_id added")
    if "year_id" in fields and "year_id" in sess_fields:
        sample = next((r["year_id"] for r in speeches if r.get("year_id")), "")
        populated = sum(1 for r in speeches if r.get("year_id"))
        report(PASS, f"year_id present — {populated}/{len(speeches)} rows populated (sample: {sample!r})")
    else:
        report(FAIL, "year_id missing from CSV headers")

    # ── FIX 3: Header leakage in speech_text ─────────────────────────────────
    print("\n[Fix 3] Header leakage in speech_text")
    leaks = [r for r in speeches if _HEADER_RE.search(r.get("speech_text", ""))]
    by_file_leaks = {}
    for r in leaks:
        by_file_leaks.setdefault(r["file_id"], []).append(r["speech_id"])
    if not leaks:
        report(PASS, "No running-header pattern found in any speech_text")
    else:
        report(FAIL, f"{len(leaks)} speech(es) contain running-header text",
               detail="; ".join(f"{f}: {len(ids)} speeches" for f, ids in by_file_leaks.items()))
        for fid, ids in list(by_file_leaks.items())[:3]:
            r = next(x for x in leaks if x["file_id"] == fid)
            m = _HEADER_RE.search(r["speech_text"])
            if m:
                excerpt = r["speech_text"][max(0, m.start() - 40): m.end() + 40]
                report(INFO, f"  Example ({fid} pos={r['position_in_session']}): {excerpt!r}")

    # ── FIX 4: Session-closing text in speeches ───────────────────────────────
    print("\n[Fix 4] Session-closing text stripped from speech_text")
    close_leaks = [r for r in speeches if _CLOSE_RE.search(r.get("speech_text", ""))]
    if not close_leaks:
        report(PASS, "No session-closing phrases found in speech_text")
    else:
        report(WARN, f"{len(close_leaks)} speech(es) still contain session-closing text",
               detail="; ".join(f"{r['file_id']} pos={r['position_in_session']}" for r in close_leaks[:5]))

    # ── FIX 5: Procedural dash text stripped ──────────────────────────────────
    print("\n[Fix 5] Procedural dash lines stripped from speech_text")
    dash_leaks = [r for r in speeches if _DASH_RE.search(r.get("speech_text", ""))]
    if not dash_leaks:
        report(PASS, "No procedural dash lines found in speech_text")
    else:
        report(WARN, f"{len(dash_leaks)} speech(es) contain dash-prefixed lines",
               detail="; ".join(f"{r['file_id']} pos={r['position_in_session']}" for r in dash_leaks[:5]))

    # ── FIX 6: Trailing verb in speaker_raw ──────────────────────────────────
    print("\n[Fix 6] Trailing verbs stripped from speaker_raw")
    trailing = [r for r in speeches if _TRAILING_VERB_RE.search(r.get("speaker_raw", ""))]
    if not trailing:
        report(PASS, "No trailing verbs found in speaker_raw")
    else:
        report(FAIL, f"{len(trailing)} speaker_raw value(s) still have trailing verbs",
               detail="; ".join(r["speaker_raw"] for r in trailing[:4]))

    # ── FIX 7: gaceta_1168 (2) — missing turn boundaries ────────────────────
    print("\n[Fix 7] gaceta_1168 (2): 'quien manifiesta/indica' patterns")
    g1168 = by_file(speeches, "gaceta_1168 (2)")
    n = len(g1168)
    if n > 9:
        report(PASS, f"gaceta_1168 (2): {n} speeches extracted (was 9 before fix)")
    elif n == 9:
        report(WARN, f"gaceta_1168 (2): still {n} speeches — mid-paragraph patterns may not have fired")
    else:
        report(FAIL, f"gaceta_1168 (2): only {n} speeches")

    # ── FIX 8: Dirección de Presidencia multi-line name ──────────────────────
    print("\n[Fix 8] gaceta_1193: Dirección de Presidencia speaker_raw")
    g1193 = by_file(speeches, "gaceta_1193")
    dir_pres = [r for r in g1193 if "Presidencia" in r.get("speaker_raw", "")
                or "Direcci" in r.get("speaker_raw", "")]
    if not dir_pres:
        report(PASS, "No 'Dirección de Presidencia' leaking into speaker_raw in gaceta_1193")
    else:
        report(FAIL, f"{len(dir_pres)} speakers still have 'Presidencia' in speaker_raw",
               detail="; ".join(r["speaker_raw"] for r in dir_pres[:3]))

    # ── FIX 9: Venus Albeiro name cleaned ────────────────────────────────────
    print("\n[Fix 9] gaceta_492 (9): Venus Albeiro speaker name")
    g492 = by_file(speeches, "gaceta_492 (9)")
    venus_rows = [r for r in g492 if "Venus" in r.get("speaker_raw", "")]
    if not venus_rows:
        report(WARN, "No 'Venus Albeiro' rows found in gaceta_492 (9)")
    else:
        for r in venus_rows:
            name = r["speaker_raw"]
            has_period = re.search(r"[A-Z][a-z]+\.\s+[A-Z]", name)
            if has_period:
                report(FAIL, f"Period still in name: {name!r}")
            else:
                report(PASS, f"Name looks clean: {name!r}")

    # ── FIX 10: gaceta_1007 skipped (all scanned) ────────────────────────────
    print("\n[Fix 10] gaceta_1007: all-scanned skip")
    g1007 = by_file(speeches, "gaceta_1007")
    if not g1007:
        report(PASS, "gaceta_1007 produced no speech rows (correctly skipped)")
    else:
        report(WARN, f"gaceta_1007 has {len(g1007)} speech rows — expected 0 (all scanned)")

    # ── INFO: gaceta_1077 (2) and gaceta_791 (6) speech count ────────────────
    print("\n[Info] Low-speech files (wider margins / cid fonts)")
    for fid, min_expected in [("gaceta_1077 (2)", 3), ("gaceta_791 (6)", 3)]:
        rows = by_file(speeches, fid)
        n = len(rows)
        if n >= min_expected:
            report(PASS, f"{fid}: {n} speeches")
        else:
            report(WARN, f"{fid}: {n} speeches — still below expected ({min_expected}+); "
                   "cid-font left-column likely unreadable")

    # ── INFO: Session dates for gaceta_1168 (2) ──────────────────────────────
    print("\n[Info] gaceta_1168 (2): header dates")
    sess_1168 = next((s for s in sessions if s["file_id"] == "gaceta_1168 (2)"), None)
    if sess_1168:
        pub = sess_1168.get("publication_date", "")
        sess = sess_1168.get("session_date", "")
        pub_ok = pub == "2020-10-23"
        sess_ok = sess == "2020-08-25"
        report(PASS if pub_ok else WARN, f"publication_date: {pub!r} (expected 2020-10-23)")
        report(PASS if sess_ok else WARN, f"session_date: {sess!r} (expected 2020-08-25)")
    else:
        report(WARN, "gaceta_1168 (2) not found in sessions.csv")

    # ── INFO: gaceta_653 (6) — no session date ───────────────────────────────
    print("\n[Info] gaceta_653 (6): not a transcript (no session date expected)")
    sess_653 = next((s for s in sessions if s["file_id"] == "gaceta_653 (6)"), None)
    if sess_653:
        sd = sess_653.get("session_date", "")
        dt = sess_653.get("document_type", "")
        report(INFO if not sd else WARN,
               f"session_date={sd!r}, document_type={dt!r}")
    else:
        report(WARN, "gaceta_653 (6) not found in sessions.csv")

    print()
    print("=" * 60)
    print("Done.")
    print()


if __name__ == "__main__":
    main()
