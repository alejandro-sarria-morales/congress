# Pattern Proposals — Gaceta del Congreso Pipeline
# Generated from v0.5 calibration run — 2026-04-20
# Total flagged: 154 across 15 files
# All proposals require human approval before being applied to segmentation_rules.md

---

## Proposal 1 — `Señor/a Presidente` without El/La article

**Failure type:** uncertain_speaker
**Files affected:** gaceta_695 (1) (1 instance confirmed), likely widespread
**Pattern observed:**
```
§Señor Presidente:
```
`_ROLE_LABELS` requires `^(?:El|La)\s+señor[a]?` or `^(?:El|La)` as optional prefix, so
plain "Señor Presidente" (without leading article) is not matched → falls through to uncertain.

**Proposed fix — `_ROLE_LABELS`:**
Add `(?:Se[ñn]or[a]?\s+)?` as an additional optional prefix before the role name:
```python
_ROLE_LABELS = re.compile(
    r"^(?:(?:El|La)\s+se[ñn]or[a]?\s+|(?:El|La)\s+|Se[ñn]or[a]?\s+)?"
    r"(Presidente[a]?|Vicepresidente[a]?|Secretari[ao]|Subsecretari[ao]"
    r"|Direcci[oó]n(?:\s+de)?\s+Presidencia|Presidencia)",
    re.IGNORECASE,
)
```

---

## Proposal 2 — `Hace uso de la palabra` / `Procede` / `Inicia la sesión` as floor-grant narrations

**Failure type:** uncertain_speaker
**Files affected:** gaceta_695 (1) (11 uncertain, most are Benjamín Niño Flórez Secretario General)
**Pattern observed (gaceta_695, 2013 committee format):**
```
§Procede doctor Benjamín Niño Flórez,
§Secretario General Comisión Ordenamiento Territorial:

§Hace uso de la palabra, honorable Representante Luis Eduardo Diazgranados Torres,
§Presidente Comisión Ordenamiento Territorial:

§Inicia la Sesión correspondiente, honorable Representante Luis Eduardo Diazgranados Torres,
§Presidente de la Comisión:
```
These are narrative openers before the named speaker — equivalent to "La Presidencia concede el uso de la palabra". `Procede` and `Hace uso de la palabra` both signal that the named person is speaking, not the narrator.

`Benjamín Niño Flórez, Secretario General Comisión Ordenamiento Territorial` hits uncertain
because `_classify_speaker` finds no known signal (not honorable, not Ministro, no El/La role prefix).

**Proposed fixes:**
1. Extend `_FLOOR_GRANT_NARRATION_RE` to include these narration forms:
```python
_FLOOR_GRANT_NARRATION_RE = re.compile(
    r"(?:concede|otorga|Contin[uú]a con|Hace uso de la palabra|Procede|Inicia la sesi[oó]n)\s*"
    r"(?:correspondiente)?[,\s]*(?:el\s+uso\s+de\s+la\s+palabra)?",
    re.IGNORECASE,
)
```
When this matches, extract the named person as speaker (already done for concede/otorga).

2. Add `Secretari[ao]\s+General` to `_MINISTER_SIGNALS` (or a new staff-role signal) so
   "Benjamín Niño Flórez, Secretario General..." is classified as `role` rather than uncertain:
```python
_MINISTER_SIGNALS = re.compile(
    r"\b(Ministro[a]?|Viceministro[a]?|Director[a]?\s+(?:General\s+)?(?:de|del?)\b"
    r"|Gobernador[a]?\s+de\b|Delegado[a]?\s+de\b|Funcionari[ao]\b"
    r"|Secretari[ao]\s+General\b)\b",
    re.IGNORECASE,
)
```

---

## Proposal 3 — Attendance list bold blocks treated as speaker attributions

**Failure type:** uncertain_speaker, speech_too_long
**Files affected:** gaceta_846 (7), gaceta_522 (1), gaceta_255 (6), gaceta_211 (1)
**Instances:** ~8 uncertain speakers, causes speech_too_long (4394 words in gaceta_846)

**Pattern observed:**
Bold headers for attendance/absence lists become the attributed speaker for everything
that follows until the next boundary:
```
§En el transcurso de la sesión se hicieron presentes los honorables Representantes:
§Dejan de asistir con excusa los honorables Senadores:
§Contestan los siguientes honorables Senadores:
```
`En el transcurso de la sesión...` is particularly damaging — it absorbs all remaining text as
one enormous speech.

**Proposed fix — extend `_FALSE_POSITIVE_PREFIXES`:**
```python
r"|En el transcurso de la sesi[oó]n se hicieron presentes"
r"|Dejan de asistir con excusa"
r"|Contestan los siguientes honorables"
r"|Con excusa los honorables"
```

---

## Proposal 4 — Procedural event headings as speaker attributions

**Failure type:** uncertain_speaker, speech_too_short
**Files affected:** gaceta_1193 (2 instances)
**Pattern observed:**
Bold procedural event headings picked up as speaker attributions:
```
§Minuto de silencio Dirección de Presidencia, Olga Lucía Velásquez Nieto:
§Himno Nacional de la República de Colombia Dirección de Presidencia, Diana ...:
```
These are procedural stage directions, not speech turns.

**Proposed fix — extend `_FALSE_POSITIVE_PREFIXES`:**
```python
r"|Minuto de silencio"
r"|Himno Nacional"
```

Also add all-caps voting headers:
```
§VOTACIÓN ACEPTACIÓN EXCUSA DIRECTOR NACIONAL DE PLANEACIÓN: Por el Sí...
```
**Add:**
```python
r"|VOTACI[OÓ]N\s"
r"|SEGUNDA VOTACI[OÓ]N"
```

---

## Proposal 5 — Feminine `Representanta` form not matched by legislator signals

**Failure type:** uncertain_speaker
**Files affected:** gaceta_1193 (1 instance: "Representanta a la Cámara, Dorina Hernández Palomino")
**Pattern observed:**
`_LEGISLATOR_SIGNALS` matches `honorable\s+Representante` but not the feminine `Representanta`.
Similarly `_extract_speaker_name` searches for `Representante` and `Senador[a]?` but not `Representanta`.

**Proposed fix — extend `_LEGISLATOR_SIGNALS`:**
```python
r"\b(honorable\s+Representante[a]?|honorable\s+Senador[a]?|honorable\s+Congresista"
```
And extend name extraction patterns to include `Representanta`:
```python
r"(?:Representante[a]?|Senador[a]?|Congresista)"
```

---

## Proposal 6 — Surname-only bold attributions (older committee format)

**Failure type:** uncertain_speaker
**Files affected:** gaceta_522 (1) (Sánchez León, Marulanda, Robledo Gómez, Casas)
**Instances:** ~4–6 per file

**Pattern observed (2019 committee format):**
```
§Sánchez León:
§Marulanda:
§Robledo Gómez:
§Casas, Comisión Séptima:
```
Speakers identified by surname only (1–2 words). No "honorable" or title prefix.
`_classify_speaker` finds no signal → uncertain. The comma + committee suffix in
"Casas, Comisión Séptima" also makes name extraction messy.

**Assessment:** Currently unavoidable without a speaker name list for each session.
These are correctly emitted as `uncertain` — the qa-agent can flag them for review.
No rule change proposed at this time. The word count and flag count are low.
**Recommended:** Accept as uncertain, monitor across full run.

---

## Proposal 7 — Partial names from column-split multi-line bold blocks

**Failure type:** uncertain_speaker
**Files affected:** gaceta_1168 (2) (Londoño Ulloa, Chinchilla, Enrique Robledo Castillo)
**Instances:** ~5 in this file

**Pattern observed:**
A two-column bold attribution where the first half of the name ends a column and the
second half begins the next — but they appear on different PDF pages or are separated
by non-bold lines, so `_BOLD_BLOCK_RE` does not merge them:
```
Page N left col:   §Londoño
Page N right col:  (speech text, not bold)
Page N+1 left col: §Ulloa:       ← new page, new bold block — only "Ulloa" is captured
```
Result: `speaker_raw = "Ulloa"` or `speaker_raw = "Londoño Ulloa"` (partial).
For "Enrique Robledo Castillo" the full name is "Jorge Enrique Robledo Castillo" — the
first word was missed because it was on the preceding line/block outside the merge window.

**Assessment:** Root cause is that multi-line bold attributions spanning a column break
or short non-bold gap are not always fully merged. Hard to fix generally without
introducing false merges. Recommend monitoring across full run.
**No rule change proposed at this time.**

---

## Proposal 8 — `(RECESO)` inside attribution text

**Failure type:** uncertain_speaker
**Files affected:** gaceta_211 (1) (1 instance: "(RECESO) Presidente")
**Pattern observed:**
A RECESO procedural marker appears inside a bold block that also contains a role label:
```
§(RECESO)
§Presidente:
```
After bold block merge: `§(RECESO) Presidente:`
`_ROLE_LABELS` does not match because `(RECESO)` precedes "Presidente".
`_extract_speaker_name` returns "(RECESO) Presidente" → uncertain.

**Proposed fix — pre-strip `(RECESO)` from bold attribution before pattern matching:**
In the bold block post-processing, strip leading `\(RECESO\)\s*` before passing to
`_classify_speaker` and `_extract_speaker_name`.

---

## Summary table

| Proposal | Failure type | Files | Impact | Action |
|---|---|---|---|---|
| 1 — Señor Presidente | uncertain | gaceta_695 + likely others | medium | **Approve** |
| 2 — Hace uso / Procede / Secretario General | uncertain | gaceta_695 (11 flags) | high | **Approve** |
| 3 — Attendance list false positives | uncertain + too_long | gaceta_846, 522, 255, 211 | high | **Approve** |
| 4 — Procedural event headings | uncertain | gaceta_1193 | low | **Approve** |
| 5 — Feminine Representanta | uncertain | gaceta_1193 | low | **Approve** |
| 6 — Surname-only attributions | uncertain | gaceta_522 | low | Accept as uncertain |
| 7 — Partial name column-split | uncertain | gaceta_1168 | medium | Monitor |
| 8 — (RECESO) inside attribution | uncertain | gaceta_211 | low | **Approve** |

---

## Proposal 10 — Preamble detector: officer list triggers false transcript-start on page 4

**Failure type:** missed_boundary (false turns from ORDER DEL DIA content)
**Files affected:** gaceta_1078 (confirmed); likely other heavily CID-encoded files
**Severity:** blocking — the ORDER DEL DIA content appears as speech turns; real debate speeches start at position 7 instead of position 1

**Root cause:**
The session officer list in gaceta_1078 is printed on page 4 in the form:
```
El Presidente,
Honorable Senador Carlos Fernando Motoa Solarte.
El Vicepresidente,
Honorable Senador Armando Benedetti Villaneda.
```
"Honorable Senador [Name]." starts at the beginning of a line, triggering the
`^Honorable (?:Representante|Senador[a]?|Congresista) [A-Z]` pattern in
`_ROLE_LABEL_PHRASES`. The preamble detector sets `transcript_start_page=4`.

Page 4, however, opens with ORDER DEL DIA sections II–VI before the officer list,
and these sections contain CID-encoded bold headings that the v0.7 fix converts to
`§[CID_ATTRIBUTION]:` boundaries — producing false speech turns for ORDER DEL DIA
items (section headings, proposición text, questionnaire items).

The real first floor grant ("La Presidencia concede el uso de la palabra al citante
honorable Senador Horacio Serpa Uribe:") is itself CID-encoded and undetectable by
the preamble detector. The actual debate begins at what is currently position 7 in
the extracted output (6,780-word speech by Horacio Serpa Uribe).

**Proposed fix — preamble_detector.py:**
Require that the "Honorable Senador/Representante" role-label line ends with `:` (a
speaker attribution colon), not `.` (an officer-list period). Change:
```python
r"^Honorable (?:Representante|Senador[a]?|Congresista) [A-Z]",
```
to:
```python
r"^Honorable (?:Representante|Senador[a]?|Congresista) [A-ZÁÉÍÓÚÑÜ][^\n:]{2,60}:",
```
This makes the pattern fire only when "Honorable Senador/Representante [Name]:" ends
with a colon — indicating it is used as a speaker attribution, not an officer list entry.

**Limitation:** Even with this fix, gaceta_1078 falls back to `detection_method:
fallback_page_2`, including pages 2–4 in the transcript. These pages contain ORDER DEL DIA
content that continues to produce false turns via `[CID_ATTRIBUTION]` boundaries. The full
fix for this file requires CID decoding at the pdf_converter level (To-Do in CLAUDE.md) so
the preamble detector can read the real floor-grant phrase on page 4.

**Impact without full fix:** false turns 1–6 are eliminated for files where the
officer-list false trigger was the only preamble-detection issue. For gaceta_1078
specifically, Proposal 10 alone does not eliminate the false turns.

**Test files:** gaceta_1078

---

---

## Proposal 11 — Preamble detector: missing "El señor Presidente concede el uso de la palabra" floor-grant

**Failure type:** missed_boundary (entire transcript sections treated as preamble)
**Files affected:** gaceta_88 (6) (confirmed — pages 4–22 missing, 79→expected ~250+ speeches); likely widespread in files using this floor-grant format
**Severity:** blocking — the majority of speeches in the file are discarded

**Root cause:**
`_FLOOR_GRANT_PHRASES` in `preamble_detector.py` contains "La Presidencia concede el uso de
la palabra" but not the variant "El señor Presidente concede el uso de la palabra". In
gaceta_88 (6), every single floor grant uses the latter form (bold §-prefixed, spanning 2–3
lines). Pages 4–22 each contain this pattern but none trigger preamble detection.

The preamble detector finally fires on page 23 — not because of a real floor grant but
because the speech body of Representante Febres contains the text:
```
Honorable Representante Sánchez, tiene usted la
palabra...
```
at the start of a line, which matches `^Honorable (?:Representante|...) [A-Z]` in
`_ROLE_LABEL_PHRASES`. This false positive sets `transcript_start_page=23`, discarding
pages 4–22 of the transcript.

**Proposed fix — `preamble_detector.py`:**
Add "El/La señor/a Presidente/a concede el uso de la palabra" to `_FLOOR_GRANT_PHRASES`:
```python
r"[Ee]l se[ñn]or[a]?\s+[Pp]residente[a]?\s+(?:le\s+)?concede el uso de la palabra",
r"[Ll]a se[ñn]ora?\s+[Pp]residenta?\s+(?:le\s+)?concede el uso de la palabra",
```

**Also affects the turn segmenter:** The bold boundary detection (`_PAT_BOLD_BOUNDARY`) already
handles bold occurrences of this phrase. No segmenter change needed for gaceta_88 (6).
However, if non-bold occurrences exist in other files, a new `_PATTERNS` entry would be
needed. Monitor after full run.

**Test files:** gaceta_88 (6) (pages 4–22 should be recovered)

---

**Remaining unaddressed flags after proposals 1–5,8:**
- speech_too_short (59): mostly genuine short procedural turns in gaceta_499 — no change needed
- speech_too_long (9): likely genuine long speeches (ministers with 20-min floor time) — acceptable
- no_date_extracted (6): header parsing issue, not segmentation
- consecutive_same_speaker (5): residual, review after next run
- few_speeches_extracted (3): gaceta_1007, gaceta_1100, gaceta_1653 — different document types

---

## Proposal 9 — CID-encoded speaker attributions: missed boundaries

**Failure type:** missed_boundary
**Files affected:** gaceta_106 (3) (speech 2007_0001_004, confirmed); gaceta_107 (6) (flagged.csv line 153, different font)
**Severity:** blocking — the misattributed speech enters the corpus under the wrong speaker

**Root cause:**
Some speaker attribution lines are typeset in a font that pdfplumber cannot map to Unicode.
pdfplumber outputs these as sequences of `(cid:N)` tokens, e.g.:
```
§(cid:40)(cid:79)(cid:3)(cid:75)(cid:82)(cid:81)(cid:82)(cid:85)(cid:68)(cid:69)(cid:79)(cid:72)...(cid:29)
```
The `§` prefix is present (pdfplumber detects the font as bold by name), so the line is correctly
identified as a bold block. However `_PAT_BOLD_BOUNDARY` requires the line to end with a literal
`:`. The colon in the original attribution is itself CID-encoded (as `(cid:29)` in gaceta_106),
so the line ends with `)` — no boundary is detected.

The `_CID_LINE_RE` in the segmenter then silently removes the line from the speech_text, so
the speech that follows is absorbed into the preceding turn with no record of the missed boundary.

**Decoding finding (gaceta_106 (3) font):**
CID values in this font follow a simple offset: `char = chr(cid_value + 29)`.
Verification:
- `(cid:40)` → 40+29=69 → `E` ✓
- `(cid:79)` → 79+29=108 → `l` ✓
- `(cid:3)` → 3+29=32 → ` ` (space) ✓
- `(cid:29)` → 29+29=58 → `:` ✓

This decodes the full line as "El honorable Representante Jorge Morales Gil:" — a valid
legislator attribution that would be detected correctly if decoded.

**Different offset in gaceta_107 (6):**
flagged.csv line 153 shows CID values of 4, 7, 22, 8... with " Mora" visible at the end —
indicating a different font with a different CID offset or encoding table. The +29 rule does
not apply universally; the offset varies by font.

**Proposed fix — turn_segmenter (targeted, no PDF re-extraction needed):**

Modify the `_CID_LINE_RE` handling: instead of silently stripping `§`-prefixed CID lines from
speech_text, detect them before boundary search and insert an `uncertain` boundary at that position.

In `segment_turns`, before `_find_all_boundaries`, add a pre-pass:

```python
_CID_BOLD_LINE_RE = re.compile(
    r"(?m)^§[^\n]*(?:\(cid:\d+\)){3,}[^\n]*$"
)

def _replace_cid_bold_with_marker(text: str) -> str:
    """
    Replace §-prefixed CID lines with a synthetic uncertain boundary marker.
    The marker §[CID_ATTRIBUTION]: is detectable by _PAT_BOLD_BOUNDARY and
    will produce an uncertain turn boundary; the QA agent diagnoses it.
    """
    return _CID_BOLD_LINE_RE.sub("§[CID_ATTRIBUTION]:", text)
```

Apply before `_find_all_boundaries`:
```python
full_text = _replace_cid_bold_with_marker(full_text)
```

`§[CID_ATTRIBUTION]:` matches `_PAT_BOLD_BOUNDARY` (starts with `§`, ends with `:`, 3–400 chars),
producing a boundary with `attribution_raw = "[CID_ATTRIBUTION]"` and `pat_type = "uncertain"`.
The resulting speech turn is emitted with `speaker_type: uncertain` and `uncertain_context`
populated, triggering QA diagnosis for manual review.

Also: remove `_CID_LINE_RE` strip from `speech_text` post-processing — CID lines that are NOT
bold (not `§`-prefixed) are already handled. Only bold CID lines need the boundary treatment.

**Alternative fix — pdf_converter (deeper but more complete):**
Attempt CID decoding in `_words_to_text_with_bold` using a font-specific offset. pdfplumber
exposes `word['fontname']`; group CID words by fontname and infer the offset by testing against
expected Spanish-text character frequency or by reading the font's ToUnicode table via PyMuPDF.
This approach would correctly decode the attribution text and eliminate the need for the
`[CID_ATTRIBUTION]` placeholder, but requires a more complex implementation and verification
across the full font inventory of the corpus.

**Recommended action:** Apply the turn_segmenter fix first (lower risk, unblocks calibration).
Defer the pdf_converter decoding approach to a follow-up after the full run reveals how many
files are affected.

**Test files:** gaceta_106 (3) (p.2 left col), gaceta_107 (6)
