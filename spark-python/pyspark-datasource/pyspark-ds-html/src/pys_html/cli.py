"""Shared argparse helpers for CLI-driven examples.

Every example under ``examples/`` that reads external HTML accepts a small, consistent
set of flags (``--input``/``--url``, ``--parser``, ``--attrs``). These helpers centralize
that boilerplate so behavior and ``--help`` text stay uniform across the whole examples
suite instead of being re-implemented (and potentially drifting) in every script.
"""

from __future__ import annotations

import argparse
import glob as glob_module
from pathlib import Path

from pys_html.config import data_path, write_html_file
from pys_html.search import SCORERS

PARSER_CHOICES = ["lxml", "html5lib", "bs4"]


def build_arg_parser(description: str) -> argparse.ArgumentParser:
    """Create an ArgumentParser that shows defaults in --help output."""
    return argparse.ArgumentParser(description=description, formatter_class=argparse.ArgumentDefaultsHelpFormatter)


def add_source_args(
    parser: argparse.ArgumentParser,
    input_help: str = "Path to a local HTML file.",
    url_help: str = "HTTP(S) URL to fetch instead of a local file.",
) -> None:
    """Add mutually exclusive --input/-i and --url/-u flags to an ArgumentParser."""
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--input",
        "-i",
        type=str,
        default=None,
        help=f"{input_help} Defaults to a bundled sample if omitted.",
    )
    group.add_argument("--url", "-u", type=str, default=None, help=url_help)


def add_multi_source_args(parser: argparse.ArgumentParser) -> None:
    """Add flags for selecting *multiple* HTML sources to merge: --inputs and/or --input-glob.

    Both may be combined; matches are de-duplicated while preserving first-seen order.
    """
    parser.add_argument(
        "--inputs",
        nargs="+",
        default=None,
        metavar="FILE",
        help="One or more local HTML files (space-separated) to read and merge.",
    )
    parser.add_argument(
        "--input-glob",
        type=str,
        default=None,
        metavar="PATTERN",
        help="Glob pattern (e.g. 'data/reports/*.html') matching multiple local HTML files.",
    )


def resolve_html_sources(args: argparse.Namespace, sample_htmls: list[tuple[str, str]]) -> list[str]:
    """Return the list of HTML sources to read: --inputs, --input-glob, or bundled samples.

    Args:
        args: Parsed CLI namespace (expected to have optional .inputs and .input_glob attributes).
        sample_htmls: Fallback ``(name, html)`` pairs written to disk if neither flag is given.

    Returns:
        A list of local file paths, in the order provided (glob matches sorted alphabetically).
    """
    sources: list[str] = []
    if getattr(args, "inputs", None):
        sources.extend(str(p) for p in args.inputs)
    if getattr(args, "input_glob", None):
        # glob.glob() (unlike Path.glob()) supports absolute patterns directly.
        sources.extend(sorted(glob_module.glob(args.input_glob)))

    if sources:
        # De-duplicate while preserving order.
        return list(dict.fromkeys(sources))

    paths: list[str] = []
    for name, html in sample_htmls:
        sample_path = data_path("file_data", "html", "samples", name)
        if not Path(sample_path).exists():
            write_html_file(sample_path, html)
        paths.append(sample_path)
    return paths


def add_parser_arg(parser: argparse.ArgumentParser, default: str = "lxml") -> None:
    """Add a --parser flag selecting the pyspark.pandas / BeautifulSoup backend."""
    parser.add_argument(
        "--parser",
        choices=PARSER_CHOICES,
        default=default,
        help="Parser flavor / backend used to read HTML.",
    )


def add_search_args(parser: argparse.ArgumentParser, default_column: str, default_query: str) -> None:
    """Add --query/--search-column/--top-k/--min-score flags for a semantic_search() example."""
    parser.add_argument(
        "--query", type=str, default=default_query, help="Search text to match against --search-column."
    )
    parser.add_argument(
        "--search-column",
        type=str,
        default=default_column,
        help="Column to search (must be present in the DataFrame's schema).",
    )
    parser.add_argument("--top-k", type=int, default=10, help="Maximum number of top-scoring rows to return.")
    parser.add_argument(
        "--min-score",
        type=float,
        default=0.3,
        help="Minimum similarity score (0.0-1.0) required to keep a row.",
    )
    parser.add_argument(
        "--scorer",
        type=str,
        default="ratio",
        choices=sorted(SCORERS),
        help=(
            "rapidfuzz scoring strategy: 'ratio' (whole-string, default), "
            "'partial_ratio' (query as substring of a longer value), "
            "'token_sort_ratio' (word-order independent), or "
            "'token_set_ratio' (token overlap, good for multi-name/list cells)."
        ),
    )


def resolve_html_source(args: argparse.Namespace, sample_html: str, sample_name: str) -> str:
    """Return the HTML source to read: --url, --input, or a freshly written sample file.

    Args:
        args: Parsed CLI namespace (expected to have optional .url and .input attributes).
        sample_html: Fallback HTML markup written to disk if neither flag is given.
        sample_name: File name (under DATA_HOME/file_data/html/samples/) for the sample.

    Returns:
        A URL string, or a local file path.
    """
    if getattr(args, "url", None):
        return str(args.url)
    if getattr(args, "input", None):
        return str(args.input)

    sample_path = data_path("file_data", "html", "samples", sample_name)
    if not Path(sample_path).exists():
        write_html_file(sample_path, sample_html)
    return sample_path


def parse_attrs(raw: str | None) -> dict[str, str] | None:
    """Parse a single ``key=value`` CLI flag into an HtmlReader-compatible attrs dict."""
    if not raw:
        return None
    key, _, value = raw.partition("=")
    return {key: value}


def print_args(args: argparse.Namespace, title: str = "CLI options") -> None:
    """Pretty-print every parsed CLI option/value pair via a Rich table.

    Call this right after ``parse_args()`` in an example so the effective configuration
    (including flags left at their defaults) is always visible in the output.

    Args:
        args: Parsed CLI namespace returned by ``argparse.ArgumentParser.parse_args()``.
        title: Table title.
    """
    from rich.table import Table

    from pys_html._logging import console

    table = Table(title=title, show_lines=True, header_style="header", expand=True)
    table.add_column("Option", style="cyan", ratio=1, no_wrap=True)
    table.add_column("Value", style="magenta", overflow="fold", ratio=3)

    for key, value in sorted(vars(args).items()):
        table.add_row(f"--{key.replace('_', '-')}", "[dim]<none>[/dim]" if value is None else str(value))

    console.print(table)
