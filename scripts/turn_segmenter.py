"""
turn_segmenter.py — Stage 4 of the Gaceta del Congreso pipeline.

Extracts individual speech turns from transcript pages.
Rule-based primary logic; uncertain cases are flagged for the QA agent.

Input:
    pages          : list of page dicts (from pdf_converter), filtered to
                     transcript pages only (from transcript_start_page onward)
    file_id        : string identifier (used in speech_id)
    session_meta   : dict from header_parser (for resolving role labels)

Output: list of speech turn dicts (see CLAUDE.md §turn-segmenter output)

IMPORTANT: Read segmentation_rules.md fresh at the top of this module.
"""

import re
import unicodedata
from pathlib import Path

RULES_PATH = Path(__file__).resolve().parent.parent / "segmentation_rules.md"

# ── Read rule version from segmentation_rules.md ─────────────────────────────

def _read_rule_version() -> str:
    try:
        text = RULES_PATH.read_text(encoding="utf-8")
        m = re.search(r"^#\s*Version:\s*(\S+)", text, re.MULTILINE)
        return m.group(1) if m else "unknown"
    except OSError:
        return "unknown"

RULE_VERSION = _read_rule_version()

# ── Normalisation helpers ─────────────────────────────────────────────────────

def _norm(s: str) -> str:
    """Normalise unicode so accented chars don't cause regex misses."""
    return unicodedata.normalize("NFC", s)


# ── Turn-boundary patterns ────────────────────────────────────────────────────
#
# Each pattern is a (regex, speaker_type, label_group_index) triple:
#   - regex        : compiled pattern (MULTILINE)
#   - speaker_type : default classification for matches of this pattern
#   - attr_group   : group index that contains the raw attribution text
#
# The regex is applied to the full joined transcript text.
# All patterns anchor to the start of a line (^).
#
# IMPORTANT: patterns are tried in order; first match at each position wins.
# More-specific patterns must come before more-general ones.

_PAT_FLOOR_GRANT = re.compile(
    r"^(La Presidencia (?:le )?(?:concede|otorga) el uso de la palabra\s*\n?"
    r"(?:al|a la) (?:honorable )?(?:Representante|Senador[a]?|Congresista)\s+"
    r"[^\n:]{1,80}(?:\n[^\n:]{1,60})?):",
    re.MULTILINE | re.IGNORECASE,
)

_PAT_CON_VENIA = re.compile(
    # Do NOT use IGNORECASE here so the uppercase check on the speaker name is real.
    # Allow optional leading dash/space (committee files prefix with "- ")
    r"^[-\s]*(Con la venia de(?:l se[ñn]or Presidente|\s+la Presidencia)[^\n]*\n"
    r"(?:palabra\s+)?"
    r"(?:el doctor |la doctora |el se[ñn]or |la se[ñn]ora |[Ll]a honorable\s+|[Ee]l honorable\s+|[A-ZÁÉÍÓÚÑÜ])[^\n:]{2,80}"
    r"(?:\n[^\n:]{2,60})?):",
    re.MULTILINE,
)

_PAT_SE_LE_CONCEDE = re.compile(
    r"^(Se le concede el uso de la palabra\s+(?:al|a la)\s+(?:honorable\s+)?"
    r"(?:Representante|Senador[a]?|Congresista)\s+[^\n:]{3,80}"
    r"(?:\n[^\n:]{3,60})?):",
    re.MULTILINE | re.IGNORECASE,
)

_PAT_TIENE_USO = re.compile(
    r"^((?:Tiene|tiene) (?:el uso de la palabra|la palabra)[^\n:]{0,80}):",
    re.MULTILINE,
)

_PAT_TIENE_PALABRA = re.compile(
    r"(?:tiene la palabra\s+(?:el|la)\s+honorable\s+"
    r"(?:Representante|Senador[a]?|Congresista)\s+"
    r"([A-ZÁÉÍÓÚÑÜ][^\n:]{3,70}(?:\n[^\n:]{3,50})?)):",
    re.IGNORECASE,
)

_PAT_TIENE_PALABRA_DOT = re.compile(
    r"[Tt]iene\s+la\s+palabra\s+(?:el\s+|la\s+)?(?:honorable\s+)?"
    r"(?:Representante|Senador[a]?|Congresista)\s+"
    r"([A-ZÁÉÍÓÚÑÜ][^\n.]{2,60}(?:\n[^\n.]{2,60})?)\.",
    re.IGNORECASE,
)

_PAT_HONORABLE_TIENE_DOT = re.compile(
    r"honorable\s+(?:Representante|Senador[a]?|Congresista)\s+"
    r"([A-ZÁÉÍÓÚÑÜ][^\n,]{2,50})"
    r",\s+tiene\s+la\s+palabra\.",
    re.IGNORECASE,
)

_PAT_QUIEN_VERBO = re.compile(
    r"(?:interviene\s+)?(?:el|la)\s+honorable\s+"
    r"(?:Senador[a]?|Representante|Congresista)\s+"
    r"([A-ZÁÉÍÓÚÑÜ][a-záéíóúñü]+(?:\s+[A-ZÁÉÍÓÚÑÜ][a-záéíóúñü]+){1,5})"
    r"[,;.]\s+quien\s+(?:manifiesta|expresa|indica|afirma|se[ñn]ala|expone)\s*:",
    re.IGNORECASE,
)

_PAT_OTORGA_QUIEN = re.compile(
    r"(?:le\s+)?otorga(?:\s+el\s+uso\s+de\s+la\s+palabra)?\s+al?\s+"
    r"(?:honorable\s+)?(?:Senador[a]?|Representante|Congresista)\s+"
    r"([A-ZÁÉÍÓÚÑÜ][^\n:]{3,80}(?:\n[^\n:]{3,60})?)"
    r"[,;]\s+quien\s+(?:manifiesta|expresa|indica|afirma|se[ñn]ala|expone)\s*:",
    re.IGNORECASE,
)

_PAT_OTORGA_DOCTOR = re.compile(
    r"otorga(?:\s+el\s+uso\s+de\s+la\s+palabra)?\s+al?\s+(?:se[ñn]or[a]?\s+)?doctor[a]?\s+"
    r"([A-ZÁÉÍÓÚÑÜ][^\n,]{3,60})"
    r"[^:]+?"
    r",?\s*quien\s+(?:manifiesta|expresa|indica|afirma|se[ñn]ala|expone)\s*:",
    re.IGNORECASE,
)

_PAT_DASH_ROLE_VERB = re.compile(
    r"^-\s+(?:[^\n,]{0,40},\s+)?"
    r"(?:el|la)\s+se[ñn]or[a]?\s+"
    r"(Presidente[a]?|Secretari[ao])"
    r"(?:\s+de\s+la\s+Comisi[oó]n)?"
    r"[,\s]+(?:expresa|indica|manifiesta|se[ñn]ala|afirma)\s*:",
    re.MULTILINE | re.IGNORECASE,
)

_PAT_DASH_DOCTOR_VERB = re.compile(
    r"^-\s+(?:el|la)\s+doctor[a]?\s+"
    r"([A-ZÁÉÍÓÚÑÜ][^\n,]{3,60})"
    r"[^:]+?"
    r"(?:expresa|indica|manifiesta|se[ñn]ala|afirma)\s*:",
    re.MULTILINE | re.IGNORECASE,
)

_PAT_HONORABLE_VERB = re.compile(
    r"(?:el|la)\s+honorable\s+(?:Senador[a]?|Representante|Congresista)\s+"
    r"([A-ZÁÉÍÓÚÑÜ][a-záéíóúñü]+(?:\s+[A-ZÁÉÍÓÚÑÜ][a-záéíóúñü]+){1,5})"
    r"[,\s]*(?:expresa|indica|manifiesta|se[ñn]ala|afirma|expone)\s*:",
    re.IGNORECASE,
)

_PAT_INTERVENCION = re.compile(
    r"^(Intervenci[oó]n de(?:l|\s+la)? (?:Representante(?:\s+a la C[aá]mara)?|Senador[a]?)"
    r"[,\s]*\n?[^\n:]{3,80}):",
    re.MULTILINE | re.IGNORECASE,
)

_PAT_DIRECCION = re.compile(
    r"^(Direcci[oó]n(?:\s+de)?\s+Presidencia,?\s*[^\n:]{3,60}"
    r"(?:\n[^\n:]{2,50})?):",
    re.MULTILINE | re.IGNORECASE,
)

# Bold turn boundary — lines marked §... by the pdf_converter because every
# word on that line is bold.  Consecutive §-lines are merged into one before
# this pattern runs (see _merge_bold_blocks).  The § is stripped from
# speech_text after segmentation.
_PAT_BOLD_BOUNDARY = re.compile(
    r"^§([^§\n]{3,400}):\s*$",
    re.MULTILINE,
)

_BOLD_BLOCK_RE = re.compile(r"(?m)^§[^\n]+(?:\n§[^\n]+)+")

_PAT_ROLE_LABEL = re.compile(
    r"^(Presidente[a]?|Vicepresidente[a]?|Secretari[ao]|Subsecretari[ao]):\s*$",
    re.MULTILINE | re.IGNORECASE,
)

# Variant: "Secretario; Name:" (semicolon separator used in some older files)
_PAT_ROLE_SEMICOLON = re.compile(
    r"^((?:Secretari[ao]|Subsecretari[ao]);?\s+[A-ZÁÉÍÓÚÑÜ][^\n:;]{3,60}):",
    re.MULTILINE,
)

_PAT_ROLE_WITH_NAME = re.compile(
    r"^((?:Presidente[a]?|Vicepresidente[a]?|Secretari[ao]|Subsecretari[ao]"
    r"|Director[a]?\s+(?:de |Nacional)?[^\n,]{0,30})\s+"
    r"[A-ZÁÉÍÓÚÑÜ][a-záéíóúñü]+(?:\s+[A-ZÁÉÍÓÚÑÜ][a-záéíóúñü]+){1,4}):",
    re.MULTILINE,
)

_PAT_HONORABLE = re.compile(
    r"^(Honorable (?:Representante|Senador[a]?|Congresista)\s+"
    r"[A-ZÁÉÍÓÚÑÜ][^\n:]{3,80}(?:\n[^\n:]{3,50})?):",
    re.MULTILINE | re.IGNORECASE,
)

_PAT_MINISTER = re.compile(
    r"^((?:Ministro[a]?|Viceministro[a]?)\s+de\s+[^\n,]{3,50},?\s*"
    r"(?:doctor[a]?\s+)?[A-ZÁÉÍÓÚÑÜ][^\n:]{3,60}"
    r"(?:\n[^\n:]{3,40})?):",
    re.MULTILINE | re.IGNORECASE,
)

_PAT_SENADOR_REPRES = re.compile(
    r"^((?:Senador[a]?|Representante)\s+[A-ZÁÉÍÓÚÑÜ][^\n:]{3,70}"
    r"(?:\n(?!-)[^\n:]{3,50})?):",
    re.MULTILINE,
)

# Catch-all for lines like "Nombre Apellido Apellido:" that look like person names.
# Requires 2+ capitalized words, avoids known false positives.
_PAT_NAMED_SPEAKER = re.compile(
    r"^([A-ZÁÉÍÓÚÑÜ][a-záéíóúñüA-Z]+(?:\s+(?:de\s+la?\s+)?[A-ZÁÉÍÓÚÑÜ][a-záéíóúñü]+){2,4}):",
    re.MULTILINE,
)

_PATTERNS: list[tuple] = [
    (_PAT_BOLD_BOUNDARY,       "uncertain"),
    (_PAT_FLOOR_GRANT,         "legislator"),
    (_PAT_CON_VENIA,           "legislator"),
    (_PAT_SE_LE_CONCEDE,       "legislator"),
    (_PAT_DASH_ROLE_VERB,      "role"),
    (_PAT_DASH_DOCTOR_VERB,    "minister"),
    (_PAT_TIENE_USO,           "uncertain"),
    (_PAT_HONORABLE_TIENE_DOT, "legislator"),
    (_PAT_TIENE_PALABRA_DOT,   "legislator"),
    (_PAT_TIENE_PALABRA,       "legislator"),
    (_PAT_QUIEN_VERBO,         "legislator"),
    (_PAT_HONORABLE_VERB,      "legislator"),
    (_PAT_OTORGA_QUIEN,        "legislator"),
    (_PAT_OTORGA_DOCTOR,       "minister"),
    (_PAT_INTERVENCION,        "legislator"),
    (_PAT_DIRECCION,           "role"),
    (_PAT_ROLE_LABEL,          "role"),
    (_PAT_ROLE_SEMICOLON,      "role"),
    (_PAT_ROLE_WITH_NAME,      "role"),
    (_PAT_HONORABLE,           "legislator"),
    (_PAT_MINISTER,            "minister"),
    (_PAT_SENADOR_REPRES,      "legislator"),
    (_PAT_NAMED_SPEAKER,       "uncertain"),
]

# Lines that start with these strings are NOT turn boundaries even if they
# end with ":".  They are list introductions or section headers.
_FALSE_POSITIVE_PREFIXES = re.compile(
    r"^(DIRECTORES?|Autores?|Ponentes?|[Ll]os honorables?|[Ll]as honorables?"
    r"|Excusas?|Incapacidades?|[Cc]ontestaron|[Ss]e registraron?|[Pp]or el|"
    r"[Cc]uestionario|[Pp]roposici[oó]n|[Pp]royecto|[Dd]ej[oó]|[Cc]on excusa|"
    r"Publicaci[oó]n|Resultado|Honorables Representantes?"
    r"|En el transcurso de la sesi[oó]n se hicieron presentes"
    r"|Dejan de asistir con excusa"
    r"|Contestan los siguientes honorables"
    r"|Con excusa los honorables"
    r"|Minuto de silencio"
    r"|Himno Nacional"
    r"|VOTACI[OÓ]N\s"
    r"|SEGUNDA VOTACI[OÓ]N"
    r"|Presidente (?:del? )?Senado"
    r"|Secretario General (?:del? )?(?:Senado|C[aá]mara)"
    r"|Publicaciones?"
    r"|Se hicieron presentes"
    r")(?:[:\s]|$)",
    re.MULTILINE,
)

# Letter reference line — "Referencia:" inside an attribution signals a formal letter
# address block (institution + Ciudad + Referencia:), never a speaker attribution.
_LETTER_REF_RE = re.compile(r"\bReferencia\b|[.,]\s*Ponente", re.IGNORECASE)


# TODO: Speaker classification signals (_LEGISLATOR_SIGNALS, _MINISTER_SIGNALS,
# _CITIZEN_SIGNALS, _ROLE_LABELS) are deferred to a separate post-processing stage.
# See qa/pattern_proposals.md for the classification signals developed during calibration.

# Chair-change patterns (update presiding officer but don't create speech turn)
_CHAIR_CHANGE_RE = re.compile(
    r"(?:Preside la sesi[oó]n (?:el|la) honorable|Asume la Presidencia)[^\n]*"
    r"([A-ZÁÉÍÓÚÑÜ][a-záéíóúñü]+(?:\s+[A-ZÁÉÍÓÚÑÜ][a-záéíóúñü]+){1,4})",
    re.IGNORECASE,
)

_PROCEDURAL_RE = re.compile(
    r"^\s*\(?\s*(RECESO|Receso|Sesi[oó]n Informal|Sesi[oó]n Formal)\s*\)?\s*$",
    re.MULTILINE,
)

_SESSION_CLOSE_RE = re.compile(
    r"(?:Se levanta|Se da por terminada?|Se termina|Con esto se da|Se da fin a)"
    r"\s+la\s+sesi[oó]n",
    re.IGNORECASE,
)

_PROCEDURAL_DASH_RE = re.compile(
    r"(?m)^-\s+[A-Z][^\n]{0,200}$"
)

_CID_LINE_RE = re.compile(r"(?m)^[^\n]*(?:\(cid:\d+\)){10,}[^\n]*$")

# Bold lines that are entirely CID-encoded (turn boundary present in PDF but undecodable).
# These are replaced with a synthetic uncertain boundary before _find_all_boundaries runs.
# Threshold of 8 tokens: lines with fewer residual CIDs are decoded instead (see _decode_cid_in_attribution).
_CID_BOLD_LINE_RE = re.compile(r"(?m)^§[^\n]*(?:\(cid:\d+\)){3,}[^\n]*$")

# Known CID → character mappings for accented characters that fall outside the +29 ASCII offset.
# Confirmed in gaceta_88(6): (cid:112) = é.
_CID_CHAR_MAP = {112: "é"}


def _decode_cid_in_attribution(text: str) -> str:
    """Decode residual CID tokens in an attribution string using the +29 ASCII offset
    and known exceptions in _CID_CHAR_MAP. Unknown CID values are left as-is."""
    def _replace(m: re.Match) -> str:
        n = int(m.group(1))
        if n in _CID_CHAR_MAP:
            return _CID_CHAR_MAP[n]
        ch = chr(n + 29)
        if ch.isprintable() and not ch.isdigit():
            return ch
        return m.group(0)
    return re.sub(r"\(cid:(\d+)\)", _replace, text)

# Vote tally detection — speech text with 3+ "Vota sí/no" lines is a vote record, not a speech.
# Kept in corpus with record_type="vote_tally" so voting patterns remain analysable.
_VOTE_TALLY_RE = re.compile(r"[Vv]ota\s+(?:s[íi]|no)\b", re.IGNORECASE)
_VOTE_TALLY_THRESHOLD = 3

# Continuation patterns
_CONTINUATION_RE = re.compile(
    r"Contin[uú]a (?:con|en|haciendo uso de) el uso de la palabra",
    re.IGNORECASE,
)


# TODO: Speaker classification (speaker_type, role_label, name extraction) is deferred
# to a separate post-processing stage. The segmenter records the raw attribution string
# and leaves all identity/role resolution for later. See pattern_proposals.md for the
# classification signals that were developed during calibration rounds.


# ── Main segmentation logic ───────────────────────────────────────────────────

def _find_all_boundaries(text: str) -> list[dict]:
    """
    Scan the full transcript text and return a sorted list of boundary dicts:
        pos        : int — character position of boundary start
        end        : int — character position just after the trailing ':'
        attribution: str — raw attribution text (before ':')
        pat_type   : str — which pattern matched
    """
    # Track which character ranges are already "owned" by a match so that
    # a multi-line pattern (e.g. Intervención...Name) doesn't also produce a
    # spurious single-line match for the Name part alone.
    claimed_ranges: list[tuple[int, int]] = []

    def _is_claimed(pos: int) -> bool:
        return any(start <= pos < end for start, end in claimed_ranges)

    boundaries = []

    for pattern, default_type in _PATTERNS:
        for m in pattern.finditer(text):
            pos = m.start()
            if _is_claimed(pos):
                continue

            attr_raw = m.group(1) if m.lastindex and m.lastindex >= 1 else m.group(0)
            attr_raw = attr_raw.replace("§", "").strip()
            # Strip leading procedural annotations like "(RECESO)" that precede a role label
            attr_raw = re.sub(r"^\(RECESO\)\s*", "", attr_raw, flags=re.IGNORECASE).strip()

            if _FALSE_POSITIVE_PREFIXES.match(attr_raw):
                continue

            if _LETTER_REF_RE.search(attr_raw):
                continue

            first_char = attr_raw.lstrip()[:1]
            if first_char and first_char == first_char.lower() and first_char.isalpha():
                continue

            attr_collapsed = " ".join(attr_raw.split())
            if len(attr_collapsed) > 450:
                continue

            claimed_ranges.append((pos, m.end()))
            boundaries.append({
                "pos": pos,
                "end": m.end(),
                "attribution": attr_raw,
                "pat_type": default_type,
            })

    boundaries.sort(key=lambda b: b["pos"])
    return boundaries


def segment_turns(
    pages: list[dict],
    file_id: str,
    session_meta: dict | None = None,
) -> list[dict]:
    """
    Extract speech turns from transcript pages.

    Parameters
    ----------
    pages        : list of page dicts (transcript pages only)
    file_id      : source file identifier
    session_meta : output of header_parser.parse_header (optional)

    Returns
    -------
    list of speech turn dicts
    """
    if not pages:
        return []

    session_meta = session_meta or {}

    # Join pages with markers
    page_texts = []
    for p in pages:
        if p.get("status") == "ok":
            page_texts.append(f"\n[PAGE {p['page_num']}]\n{p['text']}")
    full_text = _norm("\n".join(page_texts))
    full_text = re.sub(r"-\n(?=[a-z])", "", full_text)

    # Merge consecutive §-lines into one, stripping § from continuation lines,
    # so _PAT_BOLD_BOUNDARY sees a single §<attribution>: line per block.
    def _merge_bold_block(m: re.Match) -> str:
        lines = m.group(0).split("\n")
        merged = lines[0] + " " + " ".join(ln.lstrip("§") for ln in lines[1:])
        # Rejoin intra-line hyphenation artifacts: "Re- presentante" → "Representante"
        merged = re.sub(r"-\s+([a-záéíóúñü])", r"\1", merged)
        return merged
    full_text = _BOLD_BLOCK_RE.sub(_merge_bold_block, full_text)
    full_text = _CID_BOLD_LINE_RE.sub("§[CID_ATTRIBUTION]:", full_text)

    boundaries = _find_all_boundaries(full_text)

    if not boundaries:
        return []

    turns: list[dict] = []
    position = 1

    for i, bnd in enumerate(boundaries):
        # Text of this turn = everything from end of boundary marker to start of next
        speech_start = bnd["end"]
        speech_end = boundaries[i + 1]["pos"] if i + 1 < len(boundaries) else len(full_text)
        speech_raw = full_text[speech_start:speech_end].strip()

        speech_text = re.sub(r"\[PAGE \d+\]", "", speech_raw).strip()
        speech_text = _CID_LINE_RE.sub("", speech_text).strip()

        close_m = _SESSION_CLOSE_RE.search(speech_text)
        if close_m:
            speech_text = speech_text[:close_m.start()].strip()

        speech_text = _PROCEDURAL_DASH_RE.sub("", speech_text).strip()
        speech_text = re.sub(r"(?m)^§", "", speech_text).strip()

        # Check continuation — always create a new turn record (is_continuation=True)
        # so the named speaker is correctly attributed even when the previous turn
        # belongs to a different speaker (e.g. Presidente granting an extension).
        is_continuation = bool(_CONTINUATION_RE.search(bnd["attribution"]))

        # Check for procedural marker — skip turn creation
        if _PROCEDURAL_RE.match(speech_text[:50]):
            continue

        # Check for chair-change in this attribution (update state, no speech record)
        cc_match = _CHAIR_CHANGE_RE.search(bnd["attribution"])
        if cc_match:
            continue

        attr = bnd["attribution"]
        attribution_raw = _decode_cid_in_attribution(" ".join(attr.split()))

        word_count = len(speech_text.split()) if speech_text else 0

        # Skip obviously empty turns
        if word_count == 0:
            continue

        tally_hits = len(_VOTE_TALLY_RE.findall(speech_text))
        record_type = "vote_tally" if tally_hits >= _VOTE_TALLY_THRESHOLD else "speech"

        speech_id = f"{file_id}_{position:03d}"
        turns.append({
            "speech_id": speech_id,
            "position_in_session": position,
            "attribution_raw": attribution_raw,
            "speech_text": speech_text,
            "word_count": word_count,
            "is_continuation": is_continuation,
            "record_type": record_type,
            "rule_version": RULE_VERSION,
        })
        position += 1

    return turns
