"""
Tax Document Renaming & Savedown Automation
============================================
Renames and copies tax documents (1099-DIVs, K-1s, etc.) into:

  Savedowns/By Fund/    <FundFolder>/  LastName, FullName - Year - FormType - FundName.pdf
  Savedowns/By Client/  <ClientFolder>/ FundName - ClientName - Year - FormType.pdf

Usage:
  python "Tax Document Renaming & Savedown Automation.py"            # normal run
  python "Tax Document Renaming & Savedown Automation.py" --debug    # print extractions, no file moves
  python "Tax Document Renaming & Savedown Automation.py" --dry-run  # show what would happen, no file moves
"""

import re
import sys
import csv
import shutil
from pathlib import Path
from datetime import datetime

import pandas as pd
import pdfplumber
from rapidfuzz import process, fuzz

# ══════════════════════════════════════════════════════════════════════
#  CONFIGURATION  — edit these paths before running
# ══════════════════════════════════════════════════════════════════════

SAVE_LOCATION = Path(r"C:\path\to\Save Location")   # root folder containing Savedowns/
DROP_FOLDER   = Path(r"C:\path\to\drop folder")      # where you bulk-save downloads

# ══════════════════════════════════════════════════════════════════════

CONFIG_XLSX       = Path(__file__).parent / "config.xlsx"
CONFIG_CSV_CLIENTS = Path(__file__).parent / "config_clients.csv"
CONFIG_CSV_FUNDS   = Path(__file__).parent / "config_funds.csv"
LOG_FILE           = Path(__file__).parent / "unmatched_log.csv"

BY_CLIENT = SAVE_LOCATION / "Savedowns" / "By Client"
BY_FUND   = SAVE_LOCATION / "Savedowns" / "By Fund"

FUZZY_THRESHOLD = 75   # minimum score (0–100) to accept a fuzzy match

FILENAME_FUZZY_THRESHOLD = 70  # slightly looser for filename matching (no OCR noise)

# ──────────────────────────────────────────────────────────────────────
#  KNOWN FORM TYPES  (add variants as you encounter them)
# ──────────────────────────────────────────────────────────────────────
FORM_KEYWORDS = {
    "1099-DIV": ["1099-DIV", "1099DIV", "1099 DIV", "DIVIDENDS AND DISTRIBUTIONS"],
    "1099-INT": ["1099-INT", "1099INT", "1099 INT", "INTEREST INCOME"],
    "1099-B":   ["1099-B",  "1099B",   "PROCEEDS FROM BROKER"],
    "K-1":      ["SCHEDULE K-1", "SCHEDULE K1", "K-1", "K1",
                 "PARTNER'S SHARE", "SHAREHOLDER'S SHARE"],
}

# Noise suffixes/lines to strip from client names before saving
NAME_NOISE_PATTERNS = [
    r"\s*\bCO\.?\s+TTEE\b.*",          # CO TTEE and everything after
    r"\s*\bTTEE\b.*",                   # TTEE and everything after
    r"\s*\bTRUSTEE\b.*",
    r"\s*\bC\s*/?\s*O\b.*",            # C/O address lines
    r"\s*\bATTN\b.*",
    r"\s*\(\s*\d{4}\s*\).*",           # (year) suffixes
    r"\s*\b\d{4}\b\s+\w+\s+TRUST.*",  # "1998 THEO TRUST" style lines
    r"\s*\bLLC\b.*",
    r"\s*\bINC\.?\b.*",
    r"\s*\bREVOCABLE\b.*",
    r"\s*\bIRREVOCABLE\b.*",
    r"\s*\bLIVING\s+TRUST\b.*",
    r"\s*\bFAMILY\s+TRUST\b.*",
]


# ══════════════════════════════════════════════════════════════════════
#  CONFIG LOADER
# ══════════════════════════════════════════════════════════════════════

def _build_lookup(df: "pd.DataFrame", base_path: Path) -> dict:
    """Turn a clients or funds DataFrame into a fuzzy-match lookup dict."""
    lookup = {}
    for _, row in df.iterrows():
        canonical = row.get("canonical_name", "").strip()
        folder    = row.get("folder_name", canonical).strip()
        variants  = row.get("name_variants", "").strip()
        if not canonical:
            continue
        all_names = [canonical] + [v.strip() for v in variants.split(";") if v.strip()]
        for name in all_names:
            lookup[name.upper()] = {
                "canonical": canonical,
                "folder":    base_path / folder,
            }
    return lookup


def load_config() -> tuple[dict, dict]:
    """
    Load client and fund config. Tries config.xlsx first (two sheets: 'clients', 'funds').
    Falls back to config_clients.csv + config_funds.csv if xlsx is not present.

    Columns for both sources: canonical_name | folder_name | name_variants (semicolon-separated)
    """
    clients, funds = {}, {}

    if CONFIG_XLSX.exists():
        print(f"Loading config from {CONFIG_XLSX.name}")
        for sheet, target, base in [("clients", clients, BY_CLIENT), ("funds", funds, BY_FUND)]:
            try:
                df = pd.read_excel(CONFIG_XLSX, sheet_name=sheet, dtype=str).fillna("")
                target.update(_build_lookup(df, base))
            except Exception as e:
                print(f"[WARNING] Could not read '{sheet}' sheet from xlsx: {e}")
        return clients, funds

    # Fallback: CSV files
    loaded_any = False
    for csv_path, target, base, label in [
        (CONFIG_CSV_CLIENTS, clients, BY_CLIENT, "clients"),
        (CONFIG_CSV_FUNDS,   funds,   BY_FUND,   "funds"),
    ]:
        if csv_path.exists():
            print(f"Loading {label} config from {csv_path.name}")
            try:
                df = pd.read_csv(csv_path, dtype=str).fillna("")
                target.update(_build_lookup(df, base))
                loaded_any = True
            except Exception as e:
                print(f"[WARNING] Could not read {csv_path.name}: {e}")
        else:
            print(f"[WARNING] {csv_path.name} not found.")

    if not loaded_any:
        print("[WARNING] No config files found. Fuzzy matching disabled.")

    return clients, funds


# ══════════════════════════════════════════════════════════════════════
#  NAME CLEANING
# ══════════════════════════════════════════════════════════════════════

def clean_client_name(raw: str) -> str:
    """
    Given a raw multi-line client name block, return the cleaned primary name.
    e.g. "JOHN SMITH CO TTEE\n1998 THEO TRUST\nC/O BLABLA" → "John Smith"
    """
    # Take only the first non-empty line
    first_line = ""
    for line in raw.strip().splitlines():
        line = line.strip()
        if line:
            first_line = line
            break

    if not first_line:
        return raw.strip()

    cleaned = first_line.upper()
    for pattern in NAME_NOISE_PATTERNS:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE).strip()

    # Title-case the result
    return cleaned.title().strip()


def extract_last_name(name: str) -> str:
    """Return the last word of the name for alphabetical sorting."""
    parts = name.strip().split()
    return parts[-1] if parts else name


def fund_folder_dest(base: Path, client_name: str, year: str, form_type: str, fund_name: str) -> Path:
    """base / Year / FormType / LastName, FullName - Year - FormType - FundName.pdf"""
    last     = extract_last_name(client_name)
    filename = f"{last}, {client_name} - {year} - {form_type} - {fund_name}.pdf"
    return base / year / form_type / filename


def client_folder_dest(base: Path, fund_name: str, client_name: str, year: str, form_type: str) -> Path:
    """base / Year / FormType / FundName - ClientName - Year - FormType.pdf"""
    filename = f"{fund_name} - {client_name} - {year} - {form_type}.pdf"
    return base / year / form_type / filename


# ══════════════════════════════════════════════════════════════════════
#  PDF EXTRACTION
# ══════════════════════════════════════════════════════════════════════

def _group_words_into_lines(words: list, y_tolerance: int = 5) -> list:
    """Group word dicts (with 'top', 'text', 'x0') into text lines."""
    lines = []
    current_line = []
    current_top = None
    for word in words:
        if current_top is None or abs(word["top"] - current_top) <= y_tolerance:
            current_line.append(word["text"])
            current_top = word["top"]
        else:
            if current_line:
                lines.append(" ".join(current_line))
            current_line = [word["text"]]
            current_top = word["top"]
    if current_line:
        lines.append(" ".join(current_line))
    return lines


def extract_pdf_fields(pdf_path: Path, debug: bool = False) -> dict:
    """
    Extract from PDF:
      - year          (TAX YEAR box first, then positional fallback)
      - form_type     (full-text keyword scan)
      - fund_name     (payer block, top-left)
      - fund_row_name (explicit "Fund:" label in data table — most reliable)
      - client_raw    (anchored on RECIPIENT label, with heuristic fallback)
      - share_class   (CLASS I/S, CL I/S, SERIES I/S patterns)
      - raw_text
    """
    result = {
        "year":          None,
        "form_type":     None,
        "fund_name":     None,
        "fund_row_name": None,
        "client_raw":    None,
        "share_class":   None,
        "raw_text":      None,
    }

    try:
        with pdfplumber.open(pdf_path) as pdf:
            if not pdf.pages:
                return result

            page = pdf.pages[0]
            w, h = page.width, page.height
            words = page.extract_words(x_tolerance=3, y_tolerance=3)
            full_text = page.extract_text() or ""
            result["raw_text"] = full_text

            if debug:
                print(f"\n{'─'*60}")
                print(f"FILE : {pdf_path.name}")
                print(f"SIZE : {w:.0f} x {h:.0f} pts")
                print(f"TEXT :\n{full_text[:800]}")
                print(f"{'─'*60}")

            # ── Year: explicit TAX YEAR box first ────────────────────────
            m = re.search(r"TAX\s+YEAR\s+(20\d{2})", full_text, re.IGNORECASE)
            if m:
                result["year"] = m.group(1)
            else:
                year_candidates = []
                for word in words:
                    if re.fullmatch(r"20\d{2}", word["text"]):
                        if word["x0"] > w * 0.5 and word["top"] < h * 0.4:
                            year_candidates.append(word["text"])
                if year_candidates:
                    result["year"] = year_candidates[0]
                else:
                    m2 = re.search(r"(20\d{2})\d{4}", full_text) or re.search(r"\b(20\d{2})\b", full_text)
                    if m2:
                        result["year"] = m2.group(1)

            # ── Form type: full-text keyword scan ────────────────────────
            upper_text = full_text.upper()
            for form, keywords in FORM_KEYWORDS.items():
                for kw in keywords:
                    if kw.upper() in upper_text:
                        result["form_type"] = form
                        break
                if result["form_type"]:
                    break

            # ── Fund: data row (most explicit, directly labelled) ─────────
            m = re.search(r"(?:^|\n)\s*Fund:\s*(.+?)(?:\n|$)", full_text, re.IGNORECASE)
            if m:
                result["fund_row_name"] = m.group(1).strip()

            # ── Share class (CL I/S, CLASS I/S, SERIES I/S) ──────────────
            m = re.search(
                r"\bCLASS\s+([A-Z])\b|\bCL\.?\s+([A-Z])\b|\bSERIES\s+([A-Z])\b",
                full_text, re.IGNORECASE
            )
            if m:
                result["share_class"] = next(g for g in m.groups() if g).upper()

            # ── Top-left region: payer block → fund name ─────────────────
            top_left_words = [
                ww for ww in words
                if ww["x0"] < w * 0.55 and ww["top"] < h * 0.50
            ]
            top_left_words.sort(key=lambda ww: (ww["top"], ww["x0"]))
            tl_lines = _group_words_into_lines(top_left_words)

            if debug:
                print(f"TOP-LEFT LINES: {tl_lines}")

            # First line that isn't a form template label is the payer/fund name
            for line in tl_lines:
                if not re.search(r"PAYER.S\s+NAME|STREET\s+ADDRESS|CITY\s+OR\s+TOWN", line, re.IGNORECASE):
                    result["fund_name"] = line.strip()
                    break

            # ── Client name: anchor on RECIPIENT label ───────────────────
            recipient_y = None
            for ww in words:
                if "RECIPIENT" in ww["text"].upper() and ww["x0"] < w * 0.55 and ww["top"] < h * 0.5:
                    recipient_y = ww["top"]
                    break

            if recipient_y is not None:
                below_words = [
                    ww for ww in words
                    if ww["top"] > recipient_y + 2
                    and ww["top"] < recipient_y + 120
                    and ww["x0"] < w * 0.55
                ]
                below_words.sort(key=lambda ww: (ww["top"], ww["x0"]))
                below_lines = _group_words_into_lines(below_words)

                if debug:
                    print(f"RECIPIENT LINES: {below_lines}")

                client_lines = []
                for line in below_lines:
                    if re.search(r"\d", line) or re.search(r"\bRECIPIENT\b|\bPAYER\b", line, re.IGNORECASE):
                        if client_lines:
                            break
                        continue
                    client_lines.append(line)
                    if len(client_lines) >= 2:
                        break

                if client_lines:
                    result["client_raw"] = "\n".join(client_lines)

            # Fallback: heuristic address-skipping if RECIPIENT anchor missed
            if not result["client_raw"] and tl_lines:
                address_done = False
                client_lines = []
                for line in tl_lines[1:]:
                    upper = line.upper().strip()
                    is_address = bool(re.search(r"\d", line)) or len(line.split()) <= 1
                    if not address_done and is_address:
                        continue
                    else:
                        address_done = True
                        if re.match(r"C\s*/?\s*O\b", upper):
                            break
                        client_lines.append(line)
                if client_lines:
                    result["client_raw"] = "\n".join(client_lines)

    except Exception as e:
        print(f"[ERROR] Could not read {pdf_path.name}: {e}")

    return result


# ══════════════════════════════════════════════════════════════════════
#  FILENAME PARSING — year, form type, and config matching
# ══════════════════════════════════════════════════════════════════════

def _clean_filename_for_matching(stem: str) -> str:
    """
    Strip noise from a filename stem to expose meaningful name tokens.
    Removes: pure number chunks, known form type strings, years.
    """
    s = stem.upper()
    # Remove form type keywords
    for keywords in FORM_KEYWORDS.values():
        for kw in keywords:
            s = s.replace(kw.upper().replace("-", "").replace(" ", ""), " ")
            s = s.replace(kw.upper(), " ")
    # Remove years
    s = re.sub(r"\b20\d{2}\b", " ", s)
    # Remove pure number chunks (reference numbers)
    s = re.sub(r"\b\d+\b", " ", s)
    # Replace underscores, dashes, dots with spaces
    s = re.sub(r"[_\-\.]+", " ", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _sliding_window_match(text: str, lookup: dict, threshold: int) -> dict | None:
    """
    Try every 1-to-5 word window of text against lookup keys.
    Returns the best match above threshold, or None.
    """
    if not text or not lookup:
        return None

    tokens = text.split()
    keys   = list(lookup.keys())
    best_score = 0
    best_match = None

    for size in range(min(5, len(tokens)), 0, -1):
        for i in range(len(tokens) - size + 1):
            window = " ".join(tokens[i:i + size])
            match, score, _ = process.extractOne(window, keys, scorer=fuzz.token_sort_ratio)
            if score > best_score:
                best_score = score
                best_match = match

    if best_score >= threshold:
        return lookup[best_match]
    return None


def _substring_name_match(text: str, lookup: dict, min_token_length: int = 5) -> dict | None:
    """
    Fallback for concatenated filenames (e.g. MATHEOPDWYER, BlackstonePrivateCreditFundAdvisory).
    Splits each config key into tokens, checks how many appear as substrings in the
    cleaned filename text (spaces removed), and scores by total matched character length.
    Returns the highest-scoring entry above zero, or None.
    """
    if not text or not lookup:
        return None

    text_upper = text.upper().replace(" ", "")
    best_key   = None
    best_score = 0

    for key in lookup:
        tokens = [t for t in re.split(r"[\s,]+", key) if len(t) >= min_token_length]
        if not tokens:
            continue
        score = sum(len(t) for t in tokens if t.upper() in text_upper)
        if score > best_score:
            best_score = score
            best_key   = key

    return lookup[best_key] if best_key else None


def _get_ambiguous_fund_candidates(cleaned: str, lookup: dict,
                                   min_token_length: int = 5,
                                   ambiguity_ratio: float = 0.65) -> list:
    """
    Return all lookup keys that scored >= ambiguity_ratio * top_score.
    Used to detect when multiple funds share a common prefix (e.g. multiple Blackstone funds)
    so we know to open the PDF and verify.
    """
    if not cleaned or not lookup:
        return []

    text_upper = cleaned.upper().replace(" ", "")
    scored = []

    for key in lookup:
        tokens = [t for t in re.split(r"[\s,]+", key) if len(t) >= min_token_length]
        score  = sum(len(t) for t in tokens if t.upper() in text_upper)
        if score > 0:
            scored.append((score, key))

    if not scored:
        return []

    max_score = max(s for s, _ in scored)
    return [key for s, key in scored if s >= max_score * ambiguity_ratio]


def _resolve_fund_from_pdf(pdf_text: str, candidate_keys: list, lookup: dict,
                           share_class: str = None) -> dict | None:
    """
    Given ambiguous fund candidates and the full PDF text, score each candidate
    by how many of its tokens appear in the PDF (with spaces — real document text).
    share_class (e.g. "I" or "S") adds a significant score boost to candidates
    whose name contains a matching CLASS/CL indicator, clinching Blackstone-style ties.
    """
    if not pdf_text or not candidate_keys:
        return None

    upper = pdf_text.upper()
    best_key   = None
    best_score = 0

    for key in candidate_keys:
        tokens = [t for t in re.split(r"[\s,]+", key) if len(t) >= 4]
        score  = sum(len(t) for t in tokens if t.upper() in upper)
        if share_class and re.search(
            rf"\bCLASS\s+{re.escape(share_class)}\b|\bCL\.?\s*{re.escape(share_class)}\b",
            key, re.IGNORECASE
        ):
            score += 20
        if score > best_score:
            best_score = score
            best_key   = key

    return lookup[best_key] if best_key else None


def parse_filename(stem: str, clients: dict = None, funds: dict = None) -> dict:
    """
    Extract year, form type, and optionally match fund/client from the filename.
    Returns dict with keys: year, form_type, fund_match, client_match, ambiguous_fund_keys
    ambiguous_fund_keys is set when multiple funds scored similarly — signals PDF verification needed.
    """
    result = {
        "year": None, "form_type": None,
        "fund_match": None, "client_match": None,
        "ambiguous_fund_keys": [],
    }

    upper = stem.upper()

    # Year — handle both standalone YYYY and YYYYMMDD formats
    m = re.search(r"(20\d{2})\d{4}", stem)
    if not m:
        m = re.search(r"\b(20\d{2})\b", stem)
    if m:
        result["year"] = m.group(1)

    # Form type
    for form, keywords in FORM_KEYWORDS.items():
        for kw in keywords:
            if kw.upper().replace("-", "").replace(" ", "") in upper.replace("-", "").replace(" ", ""):
                result["form_type"] = form
                break
        if result["form_type"]:
            break

    cleaned = _clean_filename_for_matching(stem)
    if cleaned:
        if funds:
            candidates = _get_ambiguous_fund_candidates(cleaned, funds)
            if len(candidates) > 1:
                # Multiple similar funds — flag for PDF verification
                result["ambiguous_fund_keys"] = candidates
                result["fund_match"] = funds.get(candidates[0])  # tentative
            elif len(candidates) == 1:
                result["fund_match"] = funds.get(candidates[0])
            else:
                result["fund_match"] = (
                    _sliding_window_match(cleaned, funds, FILENAME_FUZZY_THRESHOLD)
                    or _substring_name_match(cleaned, funds)
                )

        if clients:
            result["client_match"] = (
                _sliding_window_match(cleaned, clients, FILENAME_FUZZY_THRESHOLD)
                or _substring_name_match(cleaned, clients)
            )

    return result


# ══════════════════════════════════════════════════════════════════════
#  FUZZY MATCHING
# ══════════════════════════════════════════════════════════════════════

def fuzzy_match(query: str, lookup: dict, threshold: int = FUZZY_THRESHOLD) -> dict | None:
    """
    Find the best match for query in the lookup dict keys.
    Returns the matched entry dict or None if below threshold.
    """
    if not query or not lookup:
        return None

    keys = list(lookup.keys())
    match, score, _ = process.extractOne(query.upper(), keys, scorer=fuzz.token_sort_ratio)

    if score >= threshold:
        return lookup[match]
    return None


# ══════════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════════

def init_log(log_path: Path):
    if not log_path.exists():
        with open(log_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "file", "reason", "extracted_year",
                             "extracted_form", "extracted_fund", "extracted_client"])


def write_log(log_path: Path, pdf_path: Path, reason: str, fields: dict):
    with open(log_path, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            pdf_path.name,
            reason,
            fields.get("year", ""),
            fields.get("form_type", ""),
            fields.get("fund_name", ""),
            fields.get("client_raw", ""),
        ])


# ══════════════════════════════════════════════════════════════════════
#  MAIN PROCESSING LOOP
# ══════════════════════════════════════════════════════════════════════

def process_folder(drop_folder: Path, clients: dict, funds: dict,
                   debug: bool = False, dry_run: bool = False):

    # Deduplicate by resolved lowercase path (fixes Windows *.pdf + *.PDF double-match)
    seen = {}
    for p in drop_folder.glob("*"):
        if p.suffix.lower() == ".pdf" and p.is_file():
            seen[str(p).lower()] = p
    pdfs = sorted(seen.values())

    if not pdfs:
        print(f"No PDFs found in {drop_folder}")
        return

    print(f"Found {len(pdfs)} PDF(s) in {drop_folder}\n")
    init_log(LOG_FILE)

    ok_count      = 0
    skipped_count = 0

    for pdf_path in pdfs:
        print(f"Processing: {pdf_path.name}")

        # ── Step 1: Filename signals ───────────────────────────────────
        fn              = parse_filename(pdf_path.stem, clients=clients, funds=funds)
        fn_year         = fn["year"]
        fn_form_type    = fn["form_type"]
        fn_fund_match   = fn["fund_match"]
        fn_client_match = fn["client_match"]
        ambiguous_funds = fn["ambiguous_fund_keys"]

        if debug:
            amb_note = f" (AMBIGUOUS x{len(ambiguous_funds)})" if ambiguous_funds else ""
            print(f"  [FILENAME] year={fn_year} form={fn_form_type} "
                  f"fund={'YES: ' + fn_fund_match['canonical'] if fn_fund_match else 'no match'}{amb_note} "
                  f"client={'YES: ' + fn_client_match['canonical'] if fn_client_match else 'no match'}")

        # ── Step 2: PDF extraction (always — comprehensive signal gather) ─
        fields = extract_pdf_fields(pdf_path, debug=debug)

        if debug:
            print(f"  [PDF] year={fields['year']} form={fields['form_type']} "
                  f"fund_row='{fields.get('fund_row_name') or ''}' "
                  f"fund_payer='{fields.get('fund_name') or ''}' "
                  f"share_class={fields.get('share_class') or 'none'} "
                  f"client='{fields.get('client_raw') or ''}'")

        # ── Step 3: Merge all signals (PDF authoritative where anchored) ─
        # Year: TAX YEAR box > filename > PDF generic fallback
        year      = fields["year"] or fn_year

        # Form type: PDF full-text scan > filename
        form_type = fields["form_type"] or fn_form_type

        share_class = fields.get("share_class")

        # Fund resolution priority:
        #   1. Fund: data row  (explicit label in document — most reliable)
        #   2. Ambiguous filename candidates resolved via PDF text + share class
        #   3. PDF payer block fund name
        #   4. Filename match
        fund_match = None

        if fields.get("fund_row_name"):
            fund_match = fuzzy_match(fields["fund_row_name"], funds)
            if fund_match and debug:
                print(f"  [FUND via Fund: row] {fund_match['canonical']}")

        if not fund_match and ambiguous_funds and fields.get("raw_text"):
            fund_match = _resolve_fund_from_pdf(fields["raw_text"], ambiguous_funds, funds, share_class)
            if fund_match and debug:
                print(f"  [FUND via PDF disambiguation + share class] {fund_match['canonical']}")

        if not fund_match and fields.get("fund_name"):
            fund_match = fuzzy_match(fields["fund_name"], funds)
            if fund_match and debug:
                print(f"  [FUND via PDF payer block] {fund_match['canonical']}")

        if not fund_match:
            fund_match = fn_fund_match
            if fund_match and debug:
                print(f"  [FUND via filename] {fund_match['canonical']}")

        # Client resolution priority:
        #   1. PDF RECIPIENT-anchored extraction
        #   2. Filename match
        client_match = None

        if fields.get("client_raw"):
            client_name_raw = clean_client_name(fields["client_raw"])
            client_match = fuzzy_match(client_name_raw, clients)
            if client_match and debug:
                print(f"  [CLIENT via PDF] {client_match['canonical']}")

        if not client_match:
            client_match = fn_client_match
            if client_match and debug:
                print(f"  [CLIENT via filename] {client_match['canonical']}")

        # ── Step 4: Validate ───────────────────────────────────────────
        if not year or not form_type:
            reason = f"Could not extract: {', '.join(k for k, v in [('year', year), ('form_type', form_type)] if not v)}"
            print(f"  [SKIP] {reason}")
            write_log(LOG_FILE, pdf_path, reason, {
                "year": year, "form_type": form_type,
                "fund_name":   fields.get("fund_row_name") or fields.get("fund_name"),
                "client_raw":  fields.get("client_raw"),
            })
            skipped_count += 1
            continue

        if not fund_match or not client_match:
            missing = []
            if not fund_match:
                saw = fields.get("fund_row_name") or fields.get("fund_name") or "nothing"
                missing.append(f"fund (saw: '{saw}')")
            if not client_match:
                missing.append(f"client (saw: '{fields.get('client_raw', 'nothing')}')")
            reason = f"No config match for: {', '.join(missing)}"
            print(f"  [SKIP] {reason}")
            write_log(LOG_FILE, pdf_path, reason, {
                "year": year, "form_type": form_type,
                "fund_name":  fields.get("fund_row_name") or fields.get("fund_name"),
                "client_raw": fields.get("client_raw"),
            })
            skipped_count += 1
            continue

        # ── Step 5: Build destinations ─────────────────────────────────
        fund_canonical   = fund_match["canonical"]
        client_canonical = client_match["canonical"]

        fund_dest   = fund_folder_dest(fund_match["folder"],   client_canonical, year, form_type, fund_canonical)
        client_dest = client_folder_dest(client_match["folder"], fund_canonical, client_canonical, year, form_type)

        # ── Step 6: Report ─────────────────────────────────────────────
        print(f"  Client  : {client_canonical}")
        print(f"  Fund    : {fund_canonical}")
        print(f"  Year    : {year}  |  Form: {form_type}")
        if share_class:
            print(f"  Class   : {share_class}")
        print(f"  -> Fund folder  : {fund_dest}")
        print(f"  -> Client folder: {client_dest}")

        if dry_run or debug:
            print(f"  [DRY RUN -- no files moved]")
            ok_count += 1
            print()
            continue

        # ── Step 7: Copy ───────────────────────────────────────────────
        errors = []
        for dest in (fund_dest, client_dest):
            try:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(pdf_path, dest)
            except Exception as e:
                errors.append(str(e))

        if errors:
            reason = "; ".join(errors)
            print(f"  [ERROR] {reason}")
            write_log(LOG_FILE, pdf_path, reason, {"year": year, "form_type": form_type,
                      "fund_name": fund_canonical, "client_raw": client_canonical})
            skipped_count += 1
        else:
            print(f"  [OK]")
            ok_count += 1

        print()

    print(f"\nDone. {ok_count} processed, {skipped_count} skipped/errors.")
    if skipped_count:
        print(f"See {LOG_FILE} for details on skipped files.")


# ══════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    debug   = "--debug"   in sys.argv
    dry_run = "--dry-run" in sys.argv

    if debug:
        print("=== DEBUG MODE — no files will be moved ===\n")
    elif dry_run:
        print("=== DRY RUN — no files will be moved ===\n")

    clients, funds = load_config()
    print(f"Config loaded: {len(clients)} client entries, {len(funds)} fund entries.\n")

    process_folder(DROP_FOLDER, clients, funds, debug=debug, dry_run=dry_run)
