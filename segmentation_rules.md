# Segmentation Rules — Gaceta del Congreso Corpus Pipeline
# Version: 0.9
# Last updated: 2026-04-23
# Applies to files processed under rule_version: 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8

---

## How to use this file

This file is read by two agents:
- **preamble-detector** — uses Floor-grant patterns to find transcript start
- **turn-segmenter** — uses all sections below to identify and classify turns

Both agents must read this file fresh at the start of each task.
Never rely on a cached or hardcoded version of these rules.

When rules change:
1. Bump the version number at the top of this file
2. Document the change in the changelog at the bottom
3. Mark affected already-processed files as `needs_reprocessing: true`
   in the manifest if the change is substantive

---

## Floor-grant patterns

These phrases signal that a person is being given the floor.
They mark both the **start of the transcript** (used by preamble-detector)
and **turn boundaries** (used by turn-segmenter).

Match case-insensitively. The speaker name follows the pattern.

### Primary patterns (high confidence)
```
La Presidencia concede el uso de la palabra
La Presidencia le concede el uso de la palabra
Con la venia de la Presidencia hace uso de la palabra
Con la venia de la Presidencia tiene el uso de la palabra
La Presidencia otorga el uso de la palabra
Se le concede el uso de la palabra
tiene la palabra el honorable Representante/Senador [Name]:
```

### Mid-paragraph introduction patterns (confirmed in committee files)
These appear as complete sentences, not at line start:
```
[seguidamente,] interviene el honorable senador/representante [Name], quien manifiesta:
[Name], quien expresa:
[Name], quien indica:
[Name], quien afirma:
[Name], quien señala:
```
The name precedes the verb clause. Extract the name only; strip the introduction boilerplate.

### Continuation patterns
```
Continúa con el uso de la palabra
Continúa en el uso de la palabra
Continúa haciendo uso de la palabra
```
When a continuation pattern is found:
- Set `is_continuation: true` on the new turn record
- Always create a new speech record attributed to the named person
- Do NOT append to the previous turn — the previous turn may be a different speaker
  (e.g. Presidente granting an extension between two Catherine Juvinao segments)

### Chair-change patterns (update presiding officer, do NOT create speech turn)
```
Preside la sesión el honorable
Preside la sesión la honorable
Asume la Presidencia
```
When a chair-change pattern is found:
- Extract the new presiding officer name
- Update the active `presiding_officer` for all subsequent `role_label: Presidente` turns
- Do not create a speech record for the marker line itself

---

## Role label patterns

These labels appear at the start of a line, followed by a colon,
and indicate a turn by a named role rather than a named person.
Match at line start, case-sensitively.

```
Presidente:
Presidenta:
Vicepresidente:
Vicepresidenta:
Secretario:
Secretaria:
Subsecretario:
Subsecretaria:
```

For turns attributed to `Presidente:` or `Presidenta:`:
- Resolve to the `presiding_officer` identified in the session header
- If a chair-change has occurred since the header, use the updated name
- If no presiding officer can be identified, set `speaker_type: uncertain`

For `Secretario:` / `Secretaria:`:
- Resolve to the secretary named in the session header if available
- Otherwise set `speaker_type: role` with `speaker_raw: Secretaria`

---

## Speaker type classification

After extracting the speaker string from a floor-grant pattern or
role label, classify as follows:

**`legislator`** — if the floor-grant phrase includes any of:
- `honorable Representante`
- `honorable Senadora` / `honorable Senador`
- `honorable Congresista`

**`minister`** — if the floor-grant phrase includes any of:
- `señor Ministro` / `señora Ministra`
- `señor Viceministro` / `señora Viceministra`
- `doctor [Name], Ministro`
- `doctora [Name], Ministra`

**`role`** — if attributed via a role label pattern (Presidente:,
Secretaria:, etc.) without a full name being resolvable

**`citizen`** — if the floor-grant phrase includes any of:
- `ciudadano` / `ciudadana`
- `padre cuidador` / `madre cuidadora`
- no institutional title and no `honorable` prefix

**`uncertain`** — if:
- Speaker string is longer than 100 characters
- Speaker string contains digits
- Speaker string contains `(cid:` (corrupt text)
- No pattern above matches
- Multiple possible interpretations exist

When `uncertain`, populate `uncertain_context` with 200 characters
before and after the boundary in the original text.

---

## Procedural markers

These appear in the transcript but are not speech turns.
Detect and log them but do not create speech records.

```
(RECESO)
(Receso)
RECESO
Sesión Informal
Sesión Formal
```

### Session-closing markers
When these phrases appear inside a speech turn, truncate the speech at that point:
```
Se levanta la sesión
Se da por terminada la sesión
Se termina la sesión
Con esto se da fin a la sesión
Se da fin a la sesión
```

### Procedural dash lines
Lines starting with `- ` (dash + space + uppercase) within speech text are procedural
annotations (adhesions, motions), not part of the speech. Strip them from speech_text.

Log their position in the session for structural reference.

---

## Emit rules — when NOT to create a speech record

- Lines that are only a date, page number, or session header
- The chair-change marker line itself
- Procedural markers listed above
- Continuation blocks (append to previous turn instead)
- Lines containing only `Página \d+` or `ORDEN DEL DÍA`
- The `Anexos:` footer line at the end of some sessions

---

## Uncertainty escalation

Do not call the LLM during segmentation for uncertain cases.
Instead:
1. Emit the turn with `speaker_type: uncertain`
2. Populate `uncertain_context`
3. Let the qa-agent handle diagnosis in the batch review

This keeps the segmenter fast and cost-controlled.
The qa-agent makes one targeted LLM call per flagged item
with full context, which is more effective than an in-line call
with limited context.

---

## Known formatting variants (update as QA discovers new ones)

### Variant A — Bold attribution lines (confirmed in 2022–2023 files)
In many files, full speaker attributions appear as bold lines in the PDF rather than
following a plain floor-grant sentence. The converter prefixes every entirely-bold
line with `§`. The segmenter treats any `§`-prefixed line ending with `:` as a turn
boundary after merging consecutive bold lines.

**Bold line prefix:** `§` (added by pdf_converter to every line where all words are bold)

**Bold block merging:** Consecutive `§`-prefixed lines are merged into one before
pattern matching. This handles multi-line bold attributions (e.g. a name that wraps
across two lines in the PDF column).

**False positive filtering:** The following bold headings are NOT speaker attributions
and must be filtered before creating a turn boundary:
```
Proposición:
Proyecto:
Resultado:
Publicación:
Autores:
Ponentes:
Honorables Representantes:   ← plural only; singular "Honorable Representante [Name]:" is valid
Los honorables / Las honorables
```

**Floor-grant narration in bold:** Lines like:
```
§La Presidencia concede el uso de la palabra a la honorable Representante [Name]:
§Continúa con el uso de la palabra la honorable Representante [Name]:
```
...are floor-grant narrations (Presidencia granting the floor), not Presidencia speaking.
Classify the named recipient as the speaker, not the granting body.

**Header threshold:** Bold attribution lines on interior pages may appear at y≈84 pt
(just below the header strip threshold). The converter strips words with top < 84
on pages 2+ (threshold is 84, not 85) to avoid clipping these lines.

### Variant B — Spaced header text (confirmed in multiple files)
The running header may read `G aceta del c ongreso` with internal spaces.
The header stripper handles this by y-coordinate, not string matching,
so no special handling needed in segmentation.

### Variant C — Informal session blocks (confirmed in 2022–2023 files)
Some sessions include an `(Sesión Informal)` block where citizens
and non-legislators speak. These follow the same floor-grant patterns
but speakers are classified as `citizen`. The block ends when
`(Sesión Formal)` or a return to `Presidente:` role turns appears.

---

## Changelog

### v0.9 — CID decoding in attribution_raw; raise _CID_BOLD_LINE_RE threshold to 8; vote tally detection (2026-04-23)

**Pre-calibration speech counts (v0.8 baseline):**
gaceta_88 (6): 193 | all other files unchanged from v0.8

**Changes:**
- `turn_segmenter`: Added `_CID_CHAR_MAP` and `_decode_cid_in_attribution()`. Attribution strings
  that pass through with residual CID tokens (below the threshold) are now decoded using the
  confirmed +29 ASCII offset and explicit exceptions for accented characters. Confirmed mappings:
  `(cid:93)` → z, `(cid:84)` → q (both +29 offset); `(cid:112)` → é (offset 121, accented char).
  Applied to `attribution_raw` only — speech body text is unchanged.
- `turn_segmenter`: `_CID_BOLD_LINE_RE` threshold kept at 3 tokens. An earlier draft raised it to
  8, but this caused gaceta_707 (5) to lose 7 CID_ATTRIBUTION boundaries (lines with 3–7 tokens
  no longer became [CID_ATTRIBUTION] and the CID colon still didn't match `_PAT_BOLD_BOUNDARY`).
  The gaceta_88 (6) decoding fix (z, q, é with 1–2 tokens per name) works regardless of this
  threshold since those names are always below 3 tokens and pass through to `_decode_cid_in_attribution`.
- `turn_segmenter`: Added `_VOTE_TALLY_RE` and `record_type` output field. Speech turns where
  the text contains 3+ occurrences of "Vota sí/no" (case-insensitive) are tagged
  `record_type: vote_tally` instead of `speech`. Retained in corpus for voting pattern analysis.
  Confirmed in gaceta_399 (3): vote readout turns from "Doctor Raúl Ávila Hernández" and Subsecretaria.
- `turn_segmenter`: Made `_PAT_FLOOR_GRANT` newline optional (`\s*\n` → `\s*\n?`) between
  "palabra" and "al/a la". When "pala-\nbra" is hyphen-split across lines, hyphenation removal
  joins it to "palabra al honorable..." on a single line — but the pattern previously required
  a newline at that position and missed the boundary. Confirmed in gaceta_713 (2) p.3.
- `preamble_detector`: Added `§?` prefix to all `_ROLE_LABEL_PHRASES` — bold attribution lines
  are prefixed with `§` by pdf_converter; without this the preamble detector skipped them and
  scanned to a later page. Extended Presidente/Secretaria name variant to accept comma and
  optional "honorable Representante/Senador/doctor" prefix: handles "Presidente, honorable
  Representante Luis Ramiro Ricardo Buelvas:" (gaceta_370 p.3). Confirmed: without this fix
  transcript_start_page was set to page 14 instead of page 3.
- `preamble_detector`: Apply hyphenation removal (`-\n(?=[a-z])` → `""`) to page text before
  pattern matching. Same hyphen split caused `_FLOOR_GRANT_RE` to miss "pala-\nbra" in the
  raw page text, setting `transcript_start_page` too late and discarding p.3 speeches as preamble.
- `turn_segmenter`: Added to `_FALSE_POSITIVE_PREFIXES`: `Publicaciones?` (bold ORDER DEL DÍA
  section headers split off as false turns in gaceta_300 (2)) and `Se hicieron presentes`
  (attendance-list variant; existing prefix only caught "En el transcurso de la sesión se
  hicieron presentes", not this reversed form).
- `turn_segmenter`: Extended `_LETTER_REF_RE` to also suppress attributions containing
  `[.,]\s*Ponente` — bill ponente listings in ORDER DEL DÍA readings use the form
  "Representante [Name]. Ponente(s):" which matched `_PAT_BOLD_BOUNDARY` as a false turn.
- `turn_segmenter`: Added `_LETTER_REF_RE` — attributions containing "Referencia" are suppressed
  as false positives. Bold-block merging joins the address lines of formal excuse letters
  ("Presidente Senado de la República / Ciudad / Referencia:") into a single bold attribution
  that matched `_PAT_BOLD_BOUNDARY`. "Referencia" never appears in a real speaker attribution.
  Also added "Presidente del Senado" and "Secretario General del Senado/Cámara" to
  `_FALSE_POSITIVE_PREFIXES`. Confirmed in gaceta_991 (4) positions 1–2: 3848- and 3343-word
  excuse letters were the first two records in speeches.csv.
- `turn_segmenter`: Fixed `_PAT_CON_VENIA` — added optional `(?:palabra\s+)?` before the
  speaker-title group. When "hace uso de la" wraps to the next line as "palabra el honorable...",
  the pattern previously failed because "palabra" (lowercase) didn't match any title prefix.
  Confirmed in gaceta_156 speech 0000_0001_020: "Con la venia de la Presidencia hace uso de la /
  palabra el honorable Senador Alfredo Rangel / Suárez:" was a missed boundary.
- `turn_segmenter`: Raised `_CID_LINE_RE` speech-text stripping threshold from 3 to 10 tokens.
  At 3, lines of legitimate speech content with a handful of CID-encoded characters (accented
  letters, punctuation) were being silently dropped, leaving gaps in speech_text. Confirmed in
  gaceta_707 (5) speech 2009_0001_005: "relación a los 'juglares' compositores de Colombia..."
  visible in PDF but missing from corpus. Column-extraction garbage (full CID lines) typically
  has 15–40+ tokens and is still caught at the higher threshold.
- `preamble_detector`: Added `_ADMIN_PAGE_RE` — pages containing `Registro manual` as a bold
  line are skipped when scanning for the transcript start, even if they incidentally contain
  floor-grant or role-label text. Prevents roll call pages from being mistaken for the session
  opening. Note: "ORDEN DEL DÍA" heading was considered but excluded — many sessions start
  their transcript on the same page as that heading, so skipping it causes missed speeches
  (confirmed regression in gaceta_349).

### v0.8 — Preamble detector: "El señor Presidente concede" floor-grant variant (2026-04-23)

**Pre-calibration speech counts (v0.7 baseline):**
gaceta_88 (6): 79 | all other files unchanged from v0.7

**Changes:**
- `preamble_detector`: Added "El/La señor/a Presidente/a concede el uso de la palabra"
  variants to `_FLOOR_GRANT_PHRASES`. This pattern is used as the floor-grant in gaceta_88 (6)
  (and likely other files from the same era/format) but was absent from the detector, causing
  `transcript_start_page` to be set by a false positive on page 23 ("Honorable Representante
  Sánchez, tiene usted la palabra" in speech body text). Pages 4–22 were discarded as preamble.

### v0.7 — CID-encoded attribution boundary detection (2026-04-22)

**Pre-calibration speech counts (v0.6 baseline):**
gaceta_1053: 115 | gaceta_106 (3): 270 | gaceta_107 (6): 77 | gaceta_1078: 1
gaceta_139 (2): 10 | gaceta_1431 (1): 18 | gaceta_156: 67 | gaceta_179 (9): 442
gaceta_300 (2): 241 | gaceta_339 (1): 19 | gaceta_349: 21 | gaceta_370: 53
gaceta_399 (3): 58 | gaceta_451: 109 | gaceta_707 (5): 134 | gaceta_713 (2): 24
gaceta_88 (6): 79 | gaceta_990 (3): 147 | gaceta_991 (4): 139 | **TOTAL: 2024**

**Changes:**
- `turn_segmenter`: Added `_CID_BOLD_LINE_RE` — detects §-prefixed lines containing 3+
  `(cid:N)` tokens (speaker attributions typeset in a font pdfplumber cannot decode).
  These are replaced with `§[CID_ATTRIBUTION]:` before boundary detection, producing an
  `uncertain` speech turn rather than silently absorbing the text into the preceding turn.
  Root cause: CID colon `(cid:29)` does not match the literal `:` required by
  `_PAT_BOLD_BOUNDARY`, so no boundary was created. Confirmed in gaceta_106 (3) p.2
  (Jorge Morales Gil turn), gaceta_139 (2), and gaceta_107 (6) (different CID offset).
  The +29 ASCII offset applies to the gaceta_106/139 font; gaceta_107 uses a different
  encoding — both are now caught generically by the CID token count.

### v0.6 — Pattern proposals 1–5, 8 from v0.5 calibration review (2026-04-20)

**Pre-calibration speech counts (v0.5 baseline):**
gaceta_1077 (2): 12 | gaceta_1100 (1): 2 | gaceta_1168 (2): 24 | gaceta_1193: 221
gaceta_211 (1): 160 | gaceta_255 (6): 239 | gaceta_393: 109 | gaceta_492 (9): 116
gaceta_499: 142 | gaceta_522 (1): 166 | gaceta_602 (4): 5 | gaceta_695 (1): 38
gaceta_791 (6): 16 | gaceta_846 (7): 44 | gaceta_850 (1): 583 | **TOTAL: 1878**

**Changes:**
- `_FALSE_POSITIVE_PREFIXES`: added attendance/absence list openers (`En el transcurso de
  la sesión se hicieron presentes`, `Dejan de asistir con excusa`, `Contestan los siguientes
  honorables`, `Con excusa los honorables`), procedural event headings (`Minuto de silencio`,
  `Himno Nacional`), and all-caps voting headers (`VOTACIÓN`, `SEGUNDA VOTACIÓN`)
- `_LEGISLATOR_SIGNALS`: added `Representanta` (feminine form) alongside `Representante`
- `_MINISTER_SIGNALS`: added `Secretario/a General` as a classifiable institutional role
- `_ROLE_LABELS`: added `Señor/a` as an optional leading article (e.g. `Señor Presidente:`)
- `_FLOOR_GRANT_NARRATION_RE`: extended to include `Hace uso de la palabra`,
  `Procede`, `Inicia la sesión correspondiente` — all narrate the next speaker, not self
- Bold attribution pre-processing: strip leading `(RECESO)` annotation before role classification
- Name extraction: extended `Representante`→`Representante[a]?` throughout

### v0.5 — Bold boundary detection + continuation/floor-grant fixes (2026-04-20)

**Pre-calibration speech counts (v0.4 baseline):**
gaceta_1077 (2): 12 | gaceta_1100 (1): 2 | gaceta_1168 (2): 24 | gaceta_1193: 221
gaceta_211 (1): 131 | gaceta_255 (6): 172 | gaceta_393: 83 | gaceta_492 (9): 92
gaceta_499: 93 | gaceta_522 (1): 161 | gaceta_602 (4): 4 | gaceta_695 (1): 38
gaceta_791 (6): 16 | gaceta_846 (7): 44 | gaceta_850 (1): 566 | **TOTAL: 1659**

**Changes:**
- `pdf_converter`: `HEADER_TOP_REST` lowered from 85 to 84 — recovers bold attribution lines
  at y≈84.9 pt that were being silently stripped by the header zone filter (confirmed in gaceta_1168 (2))
- `turn_segmenter`: Added `_FLOOR_GRANT_NARRATION_RE` pre-check in `_classify_speaker` —
  "La Presidencia concede/otorga el uso de la palabra" and "Continúa con el uso de la palabra"
  are floor-grant narrations; classify the named recipient, not the granting body
- `turn_segmenter`: Continuation turns (`Continúa con el uso de la palabra`) now always
  create a new speech record with `is_continuation=True` instead of appending to the
  previous turn. Previous behavior caused Catherine Juvinao's resumed speech to be
  appended to an intervening Presidente turn.
- `turn_segmenter`: Fixed `_FALSE_POSITIVE_PREFIXES` — changed `Honorables? Representantes?`
  to `Honorables Representantes?` (require plural). Singular "Honorable Representante [Name]:"
  is a valid bold speaker attribution and was being incorrectly silenced. This fix recovers
  the majority of missing turns in files using bold attribution format.
- `segmentation_rules.md`: Documented bold boundary detection in Variant A section

### v0.4 — Period-terminated floor grants (committee files, older format)
- Added `_PAT_TIENE_PALABRA_DOT`: matches "tiene la palabra [el/la] [honorable] [title] [Name]." ending with period instead of colon
- Added hyphen normalization before pattern matching: `re.sub(r"-\n(?=[a-z])", "", full_text)` — rejoins typographic line-break hyphens in the full transcript before any regex runs
- `_PAT_TIENE_PALABRA_DOT` ordered before `_PAT_TIENE_PALABRA` (colon) in _PATTERNS to prevent cid colons from being used as false boundary terminators
- Added `_CID_LINE_RE` to strip lines of 3+ cid sequences from speech_text (left-column garbage that leaks after period-terminated boundaries)
- Added `_PAT_HONORABLE_TIENE_DOT`: reverse-order form "honorable [title] [Name], tiene la palabra." where name precedes the floor-grant phrase (confirmed in gaceta_1077 (2))
- Impact: gaceta_1077 (2) 1→12, gaceta_791 (6) 2→16, gaceta_1100 (1) 2→8, gaceta_499 153→164

### v0.3 — Calibration round 2 fixes
- `_PAT_CON_VENIA` now allows optional leading dash/space (committee files use "- Con la venia...")
- `_PAT_CON_VENIA` second line now also accepts "la honorable / el honorable" prefixes
- Added `_PAT_OTORGA_QUIEN` for "le otorga el uso de la palabra al honorable [Name], quien [verb]:" (committee format)
- `_SESSION_CLOSE_RE` anchor removed — now catches session-closing phrases mid-sentence
- `_extract_speaker_name`: added checks for Presidente mention lines (period-terminated or "tiene el uso") followed by actual speaker attribution
- Header leakage fully resolved by regenerating calibration JSONs with fixed pdf_converter

### v0.2 — Calibration round 1 feedback
- Added `tiene la palabra el honorable Representante/Senador [Name]:` floor-grant variant
  (fixes 0-speech extraction in gaceta_1077 and gaceta_791 format files)
- Added mid-paragraph introduction patterns (`quien manifiesta/expresa/indica/afirma/señala`)
- Fixed `Dirección de Presidencia` pattern to allow name spanning two lines
- Trailing verbs/phrases after name (`, expresa`, `, da apertura a la sesión`) stripped from speaker_raw
  — applies to any punctuation (comma, semicolon, period) before a verb
- Periods within extracted names are cleaned (e.g. `Venus Albeiro. Silva Gomez` → `Venus Albeiro Silva Gomez`)
- Role labels no longer resolved to named persons; speaker_raw keeps the attribution as-is
- Session-closing markers added: truncate last speech at close phrase
- Procedural dash lines (` - A la anterior solicitud...`) stripped from speech text
- Removed: act_number, presiding_officer, legislature from output schema
- Added: year-based file_id (`{year}_{n:04d}` sorted by session_date within year)

### v0.1 — Initial version
- Established from inspection of three sample files (gaceta_09.pdf,
  gaceta_10(1).pdf, gaceta_10(2).pdf)
- Covers floor-grant patterns confirmed in 2021–2024 files
- Known gap: patterns for files pre-2005 not yet validated
- Known gap: plenary session patterns may differ from committee patterns
