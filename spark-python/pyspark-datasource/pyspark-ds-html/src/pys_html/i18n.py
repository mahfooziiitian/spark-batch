"""Bilingual (Hindi + English) column header labels and Devanagari transliteration helpers.

HTML tables scraped from Hindi-language government/land-record sites (e.g. jamabandi/mutation
registers) carry Hindi column headers with no English equivalent in the source markup. This
module maps a small set of commonly seen Hindi headers to their English meaning, purely for
building human-readable "Hindi / English" labels when *printing* results — it never renames or
mutates the underlying DataFrame's actual column names.

It also provides ``transliterate_devanagari()``, a best-effort Devanagari -> Latin-script
converter (via ``indic_transliteration``) used by :mod:`pys_html.search` so a query typed in
Devanagari can still fuzzy-match Latin-script data (e.g. transliterated Muslim names like
"RAFIK" that appear alongside Devanagari names in the same real-world column), and vice versa.
"""

from __future__ import annotations

from indic_transliteration import sanscript
from indic_transliteration.sanscript import transliterate

# Known Hindi header -> English translation. Extend as new column headers are encountered.
HINDI_TO_ENGLISH_HEADERS: dict[str, str] = {
    "क्रम संख्या": "Serial No.",
    "रैयत का नाम": "Tenant Name",
    "खाता संख्या": "Account No.",
    "भाग वर्त्तमान": "Part (Current)",
    "पृष्ठ संख्या वर्त्तमान": "Page No. (Current)",
    "जमाबन्दी संख्या": "Jamabandi No.",
    "कंप्यूटरीकृत जमाबन्दी संख्या": "Computerized Jamabandi No.",
    "देखे": "Viewed",
}

# Non-Hindi/technical column names that also benefit from a friendlier display label.
EXTRA_HEADER_LABELS: dict[str, str] = {
    "_source_file": "Source File",
    "_score": "Match Score",
}


def bilingual_header(column: str) -> str:
    """Return a display label combining the Hindi header (if any) with its English meaning.

    Args:
        column: Raw DataFrame column name (as extracted from the HTML table).

    Returns:
        ``"<hindi> / <english>"`` if a translation is known, the column name mapped through
        ``EXTRA_HEADER_LABELS`` if it's a known technical column, or the column name unchanged
        otherwise.
    """
    if column in HINDI_TO_ENGLISH_HEADERS:
        return f"{column} / {HINDI_TO_ENGLISH_HEADERS[column]}"
    return EXTRA_HEADER_LABELS.get(column, column)


def has_devanagari(text: str) -> bool:
    """Return True if ``text`` contains at least one Devanagari (Unicode block U+0900-U+097F) char."""
    return any("\u0900" <= ch <= "\u097f" for ch in text)


def transliterate_devanagari(text: str) -> str:
    """Best-effort transliteration of Devanagari text into a phonetic Latin-script spelling.

    Uses the ITRANS scheme (via ``indic_transliteration``), which is close to how Hindi names
    are commonly typed/transliterated in English (e.g. "लक्ष्मी" -> "lakShmI"). Returns ``text``
    unchanged if it contains no Devanagari characters, so it's safe to call on any string
    (Latin-script, mixed, or already-transliterated).
    """
    if not has_devanagari(text):
        return text
    return str(transliterate(text, sanscript.DEVANAGARI, sanscript.ITRANS))
