"""Lightweight, typo-tolerant "semantic" search over a Spark DataFrame column.

Spark has no built-in fuzzy/semantic search, and pulling in a full embeddings model (e.g.
sentence-transformers) is overkill for matching short fields like names or IDs extracted from
HTML tables. Instead, this module scores every row's text similarity to a query using
``rapidfuzz.fuzz.ratio()`` (a fast, C++-backed Levenshtein-based ratio) via a Spark UDF —
tolerant of spelling variations/typos (e.g. Hindi transliteration differences) without needing
a model download.

It's also transliteration-aware: real-world Hindi land-record data mixes Devanagari names with
Latin-script transliterated names in the *same* column (e.g. "लक्ष्मी राय" alongside "RAFIK").
A Devanagari query is transliterated to Latin script (and vice versa) so it can still fuzzy-match
values written in the other script.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from rapidfuzz import fuzz

from pys_html.i18n import transliterate_devanagari

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.udf import UserDefinedFunctionLike

from pys_html._logging import get_logger

logger = get_logger("search")

DEFAULT_SCORE_COLUMN = "_score"
DEFAULT_SCORER = "ratio"

# Whole-string scorer: best for short, single-token/name fields where the entire value should
# resemble the query (e.g. "RAFIK" vs "Rafik"). Penalizes length differences heavily, so it
# under-scores a short query against a long, multi-word combined field.
#
# Substring scorer: finds the best-aligned substring of the (longer) value against the query, so
# a short query like "Rafik" scores highly against "MOHAMMAD FUL HASN URF RAFI ALAM" as long as
# some substring closely matches.
#
# Scorer for values with reorderable tokens: splits both strings into words, sorts them, then
# compares — so word order differences (e.g. "Raya Roshan" vs "Roshan Raya") don't hurt the
# score.
#
# Scorer for multi-word/list-style fields: compares the set of tokens in each string, ignoring
# duplicates/order and largely ignoring extra tokens in the longer string — best for combined
# name lists (e.g. "1.MD AKBAR ALI 2.AJHAR ALI" containing "AKBAR ALI" as one of several names).
SCORERS: dict[str, Callable[[str, str], float]] = {
    "ratio": fuzz.ratio,
    "partial_ratio": fuzz.partial_ratio,
    "token_sort_ratio": fuzz.token_sort_ratio,
    "token_set_ratio": fuzz.token_set_ratio,
}


def _normalize(text: str) -> str:
    """Collapse whitespace and casefold, so formatting/case differences don't hurt the score.

    ``str.casefold()`` (rather than ``.lower()``) is used for more aggressive, locale-agnostic
    case normalization; it's a no-op for scripts without case (e.g. Devanagari/Hindi) and fixes
    otherwise-exact matches like "Rafik" vs "RAFIK" scoring artificially low.
    """
    return " ".join(text.split()).strip().casefold()


def _script_variants(text: str) -> tuple[str, ...]:
    """Return (normalized_original,) or (normalized_original, normalized_transliterated).

    Devanagari text also gets a transliterated Latin-script variant so it can be compared
    against Latin-script values (e.g. a Devanagari query vs. a transliterated Latin name in the
    same column), and vice versa. Returns a single-item tuple when there's nothing to
    transliterate, to avoid redundant comparisons.
    """
    normalized = _normalize(text)
    transliterated = _normalize(transliterate_devanagari(text))
    if transliterated == normalized:
        return (normalized,)
    return (normalized, transliterated)


def _similarity(query: str, scorer: str) -> UserDefinedFunctionLike:
    """Build a Spark UDF that scores a column's text against a fixed query string.

    Raises:
        ValueError: If ``scorer`` isn't one of the supported ``SCORERS`` names.
    """
    try:
        scorer_fn = SCORERS[scorer]
    except KeyError as exc:
        raise ValueError(f"Unknown scorer '{scorer}'. Available scorers: {sorted(SCORERS)}") from exc

    query_variants = _script_variants(query)

    def score(value: str | None) -> float:
        if value is None:
            return 0.0
        value_variants = _script_variants(value)
        # rapidfuzz scorers return 0-100; normalize to 0.0-1.0 to keep the existing score scale.
        # Take the best score across script variants (original/transliterated query vs.
        # original/transliterated value) so cross-script matches (e.g. a Devanagari query
        # against a Latin-script name) aren't penalized.
        return max(scorer_fn(q, v) for q in query_variants for v in value_variants) / 100.0

    return F.udf(score, DoubleType())


def semantic_search(
    df: DataFrame,
    column: str,
    query: str,
    top_k: int = 10,
    min_score: float = 0.0,
    score_column: str = DEFAULT_SCORE_COLUMN,
    scorer: str = DEFAULT_SCORER,
) -> DataFrame:
    """Rank rows of ``df`` by text similarity between ``column`` and ``query``.

    Uses ``rapidfuzz`` (no embeddings/model download required) so it works fully offline and
    tolerates minor spelling/typo variations — handy for searching names or free-text fields
    extracted from HTML tables (including non-Latin scripts like Hindi).

    Args:
        df: Spark DataFrame to search.
        column: Name of the text column to compare against ``query``.
        query: Search text.
        top_k: Maximum number of top-scoring rows to return.
        min_score: Minimum similarity score (0.0-1.0) required to keep a row.
        score_column: Name of the similarity-score column added to the result.
        scorer: Name of the rapidfuzz scoring strategy to use. One of:

            * ``"ratio"`` (default) — whole-string similarity; best for short fields
              (e.g. a single name) that should closely resemble the query end-to-end.
            * ``"partial_ratio"`` — best-aligned substring similarity; use when the query is a
              short fragment that may appear inside a longer combined value.
            * ``"token_sort_ratio"`` — like ``"ratio"`` but ignores word order.
            * ``"token_set_ratio"`` — token-set overlap; use for multi-word/list-style fields
              (e.g. several names combined in one cell) where only some tokens should match.

    Returns:
        A new DataFrame with an added ``score_column``, filtered to ``min_score`` and
        sorted by score descending, limited to ``top_k`` rows.

    Raises:
        ValueError: If ``column`` is not present in ``df``, or ``scorer`` is unknown.
    """
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame. Available columns: {df.columns}")

    logger.info(
        "Semantic search: column=%s query=%r scorer=%s top_k=%d min_score=%.2f",
        column,
        query,
        scorer,
        top_k,
        min_score,
    )

    scored = df.withColumn(score_column, _similarity(query, scorer)(F.col(column)))
    return scored.filter(F.col(score_column) >= min_score).orderBy(F.col(score_column).desc()).limit(top_k)
