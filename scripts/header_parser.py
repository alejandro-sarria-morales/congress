"""
header_parser.py — Stage 3 of the Gaceta del Congreso pipeline.

Extracts session metadata from preamble pages via LLM.

Backend selection (env vars):
    HEADER_BACKEND   : "ollama" (default) | "anthropic"
    OLLAMA_MODEL     : model tag for Ollama (default: llama3.1:8b)
    OLLAMA_HOST      : Ollama base URL (default: http://localhost:11434)
    ANTHROPIC_API_KEY: required only when HEADER_BACKEND=anthropic

Falls back to rule-based extraction if the LLM is unreachable.
"""

import json
import os
import re
import urllib.request
import urllib.error

HEADER_BACKEND  = os.environ.get("HEADER_BACKEND", "ollama")
OLLAMA_MODEL    = os.environ.get("OLLAMA_MODEL", "llama3.1:8b")
OLLAMA_HOST     = os.environ.get("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

_SYSTEM_PROMPT = """\
Eres un asistente especializado en analizar documentos oficiales del Congreso de Colombia \
(Gaceta del Congreso). Se te dará el texto del preámbulo de una Gaceta y debes extraer \
metadatos estructurados. Responde ÚNICAMENTE con un objeto JSON válido, sin explicaciones, \
sin markdown, sin comillas adicionales.\
"""

_USER_PROMPT_TEMPLATE = """\
Extrae los siguientes campos del texto del preámbulo. Para campos que no puedas determinar, \
usa null. Para los campos de confianza usa "high", "medium" o "low".

Campos requeridos:
- gaceta_number: número de la Gaceta (solo el número, sin "N°" ni "Número")
- publication_date: fecha de publicación en formato YYYY-MM-DD
- publication_date_confidence: high/medium/low
- session_date: fecha de la sesión en formato YYYY-MM-DD
- session_date_confidence: high/medium/low
- chamber: "camara" si es Cámara de Representantes, "senado" si es Senado, "mixed" si es conjunta, "unknown" si no se puede determinar
- committee: nombre completo del comité o comisión (null si es plenaria)
- committee_confidence: high/medium/low
- document_type: uno de "session_transcript", "plenary_transcript", "bill_text", "report", "index", "other"
- date_source: siempre "header_extracted"

Texto del preámbulo:
{preamble_text}
"""

# ── Rule-based fallback ───────────────────────────────────────────────────────

_GACETA_NUM_RE  = re.compile(r"[Nn][°ºo\.]\s*(\d[\d.,]*)", re.IGNORECASE)
_CHAMBER_RE     = re.compile(
    r"(C[aá]mara de Representantes|Senado de la Rep[uú]blica|comisiones? conjuntas?)",
    re.IGNORECASE,
)
_COMMITTEE_RE = re.compile(
    r"COMISI[OÓ]N\s+([A-ZÁÉÍÓÚÑÜ][^\n]{5,80}?)(?:\n|PERMANENTE|$)",
    re.IGNORECASE,
)
_MONTH_MAP = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
}
_DATE_LONG_RE = re.compile(
    r"(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|"
    r"septiembre|octubre|noviembre|diciembre)\s+de\s+(\d{4})",
    re.IGNORECASE,
)


def _rule_based_extract(preamble_text: str) -> dict:
    result: dict = {f: None for f in [
        "gaceta_number", "publication_date", "publication_date_confidence",
        "session_date", "session_date_confidence", "chamber", "committee",
        "committee_confidence", "document_type", "date_source",
    ]}
    result["date_source"] = "header_extracted"
    result["document_type"] = "unknown"

    m = _GACETA_NUM_RE.search(preamble_text)
    if m:
        result["gaceta_number"] = m.group(1).replace(".", "").replace(",", "")

    acta_m = re.search(r"ACTA\s+[Nn][°ºo]?[Ú]?MERO?\s+(\d+)", preamble_text, re.IGNORECASE)
    if acta_m:
        result["document_type"] = "session_transcript"

    dates = _DATE_LONG_RE.findall(preamble_text)
    def _fmt(t):
        day, month_es, year = t
        month = _MONTH_MAP.get(month_es.lower())
        return f"{year}-{month}-{int(day):02d}" if month else None

    if len(dates) >= 1:
        result["publication_date"] = _fmt(dates[0])
        result["publication_date_confidence"] = "low"
    if len(dates) >= 2:
        result["session_date"] = _fmt(dates[1])
        result["session_date_confidence"] = "low"

    m = _CHAMBER_RE.search(preamble_text)
    if m:
        val = m.group(1).lower()
        if "mara" in val:
            result["chamber"] = "camara"
        elif "senado" in val:
            result["chamber"] = "senado"
        elif "conjunta" in val:
            result["chamber"] = "mixed"

    m = _COMMITTEE_RE.search(preamble_text)
    if m:
        result["committee"] = m.group(0).strip().rstrip(":\n")
        result["committee_confidence"] = "low"

    return result


def _clean_llm_json(raw: str) -> str:
    """Strip markdown fences and thinking tags some models emit."""
    raw = raw.strip()
    # Remove <think>...</think> blocks (qwen3 thinking mode)
    raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
    # Strip ```json ... ``` fences
    raw = re.sub(r"^```[a-z]*\n?", "", raw)
    raw = re.sub(r"\n?```$", "", raw).strip()
    # If the model prepended text before the JSON, find the first '{'
    brace = raw.find("{")
    if brace > 0:
        raw = raw[brace:]
    return raw


# ── Ollama backend ────────────────────────────────────────────────────────────

def _call_ollama(preamble_text: str) -> dict:
    url = f"{OLLAMA_HOST}/api/generate"
    full_prompt = (
        _SYSTEM_PROMPT
        + "\n\n"
        + _USER_PROMPT_TEMPLATE.format(preamble_text=preamble_text[:4000])
    )
    payload = json.dumps({
        "model": OLLAMA_MODEL,
        "prompt": full_prompt,
        "stream": False,
        "options": {"temperature": 0},
    }).encode()

    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    for attempt in range(2):
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read())
        raw = _clean_llm_json(body.get("response", ""))
        try:
            result = json.loads(raw)
            result["extraction_status"] = "llm_ok"
            return result
        except json.JSONDecodeError:
            continue

    raise ValueError("Ollama returned non-JSON after 2 attempts")


# ── Anthropic backend ─────────────────────────────────────────────────────────

def _call_anthropic(preamble_text: str) -> dict:
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    for attempt in range(2):
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=1024,
            system=_SYSTEM_PROMPT,
            messages=[{
                "role": "user",
                "content": _USER_PROMPT_TEMPLATE.format(preamble_text=preamble_text[:4000]),
            }],
        )
        raw = _clean_llm_json(response.content[0].text)
        try:
            result = json.loads(raw)
            result["extraction_status"] = "llm_ok"
            return result
        except json.JSONDecodeError:
            continue

    raise ValueError("Anthropic returned non-JSON after 2 attempts")


# ── Public entry point ────────────────────────────────────────────────────────

def parse_header(preamble_text: str, file_id: str = "") -> dict:
    """
    Extract session metadata from preamble text.

    Backend is chosen by HEADER_BACKEND env var (default: ollama).
    Falls back to rule-based extraction on any error.
    """
    try:
        if HEADER_BACKEND == "rule_based":
            result = _rule_based_extract(preamble_text)
            result["extraction_status"] = "rule_based"
            return result
        elif HEADER_BACKEND == "anthropic":
            if not ANTHROPIC_API_KEY:
                raise EnvironmentError("ANTHROPIC_API_KEY not set")
            return _call_anthropic(preamble_text)
        else:
            return _call_ollama(preamble_text)

    except urllib.error.URLError:
        result = _rule_based_extract(preamble_text)
        result["extraction_status"] = "rule_based_ollama_unreachable"
        return result
    except Exception as exc:
        result = _rule_based_extract(preamble_text)
        result["extraction_status"] = f"rule_based_error:{type(exc).__name__}"
        return result
