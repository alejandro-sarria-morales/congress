"""
preamble_detector.py — Stage 2 of the Gaceta del Congreso pipeline.

Identifies the first page of transcript content (where the first person
is formally given the floor or begins speaking in a session role).

Input:  list of page dicts from pdf_converter
Output: dict with transcript_start_page, preamble_pages, detection_method
"""

import re
from pathlib import Path

# Read floor-grant patterns from segmentation_rules.md at import time.
# The turn_segmenter also reads this file; we share the same source of truth.
_RULES_PATH = Path(__file__).resolve().parent.parent / "segmentation_rules.md"

# Floor-grant phrases that mark the formal start of a session transcript.
# Derived from segmentation_rules.md §Floor-grant patterns.
_FLOOR_GRANT_PHRASES = [
    r"La Presidencia concede el uso de la palabra",
    r"La Presidencia le concede el uso de la palabra",
    r"Con la venia de la Presidencia",
    r"La Presidencia otorga el uso de la palabra",
    r"Se le concede el uso de la palabra",
    r"tiene el uso de la palabra",
    r"Tiene el uso de la palabra",
    r"[Ee]l se[ñn]or[a]?\s+[Pp]residente[a]?\s+(?:le\s+)?concede el uso de la palabra",
    r"[Ll]a se[ñn]ora?\s+[Pp]residenta?\s+(?:le\s+)?concede el uso de la palabra",
]

# Role-label patterns that also signal the start of transcript content.
# Standalone bare-role patterns (Presidente:, Secretaria:) do NOT use §? — those forms appear
# in officer lists on page 1 as bold lines and would fire too early.
# The combined "Presidente, honorable Representante [Name]" format is NOT included here
# because it appears identically in officer lists and transcript attributions — using it
# caused false early starts in gaceta_1431(1), gaceta_339(1), gaceta_451.
_ROLE_LABEL_PHRASES = [
    r"^Presidente[a]?:\s*$",
    r"^Vicepresidente[a]?:\s*$",
    r"^Secretari[ao]:\s*$",
    r"^Subsecretari[ao]:\s*$",
    r"^§?Direcci[oó]n de Presidencia,",
    r"^Intervenci[oó]n del? (?:Representante|Senador)",
    r"^Honorable (?:Representante|Senador[a]?|Congresista) [A-Z]",
]

_FLOOR_GRANT_RE = re.compile(
    "|".join(_FLOOR_GRANT_PHRASES), re.IGNORECASE
)

_ROLE_LABEL_RE = re.compile(
    "|".join(_ROLE_LABEL_PHRASES), re.MULTILINE
)

# Pages matching these patterns are still administrative content even if they
# incidentally contain floor-grant or role-label text.
# A: ORDER DEL DÍA heading on its own line (agenda listing, not session transcript).
# B: "Registro manual" bold line (roll call page).
_ADMIN_PAGE_RE = re.compile(
    r"(?m)^§?Registro manual\b",
    re.IGNORECASE,
)


def detect_preamble(pages: list[dict]) -> dict:
    """
    Scan pages in order and return the first page that contains transcript content.

    Parameters
    ----------
    pages : list of page dicts (from pdf_converter.convert_pdf)

    Returns
    -------
    dict with:
        transcript_start_page : int (1-indexed) or None
        preamble_pages        : list[int]
        detection_method      : str
    """
    ok_pages = [p for p in pages if p.get("status") == "ok"]

    if not ok_pages:
        return {
            "transcript_start_page": None,
            "preamble_pages": [],
            "detection_method": "no_extractable_text",
        }

    for page in ok_pages:
        text = page.get("text", "")
        text = re.sub(r"-\n(?=[a-z])", "", text)
        if _ADMIN_PAGE_RE.search(text):
            continue
        if _FLOOR_GRANT_RE.search(text) or _ROLE_LABEL_RE.search(text):
            start = page["page_num"]
            preamble = [p["page_num"] for p in ok_pages if p["page_num"] < start]
            return {
                "transcript_start_page": start,
                "preamble_pages": preamble,
                "detection_method": "floor_grant_pattern",
            }

    # Fallback: no pattern found — use page 2 (skip masthead/header page)
    fallback = ok_pages[1]["page_num"] if len(ok_pages) > 1 else ok_pages[0]["page_num"]
    preamble = [p["page_num"] for p in ok_pages if p["page_num"] < fallback]
    return {
        "transcript_start_page": fallback,
        "preamble_pages": preamble,
        "detection_method": "fallback_page_2",
    }
