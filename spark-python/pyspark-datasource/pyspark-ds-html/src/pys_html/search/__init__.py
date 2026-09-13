"""Lightweight, typo-tolerant text search over Spark DataFrames."""

from pys_html.search._search import SCORERS, semantic_search

__all__ = ["SCORERS", "semantic_search"]
