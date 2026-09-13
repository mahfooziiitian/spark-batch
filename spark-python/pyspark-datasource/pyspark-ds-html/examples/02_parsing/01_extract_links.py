"""Extract links from a Spark column of raw HTML strings using pys_html.parsing.

Usage:
    # Uses the bundled two-row sample by default
    uv run python examples/02_parsing/01_extract_links.py

    # Extract links from your own HTML string or file instead (single row)
    uv run python examples/02_parsing/01_extract_links.py --html '<a href="/x">X</a>'
    uv run python examples/02_parsing/01_extract_links.py --input path/to/page.html
"""

import argparse
from pathlib import Path

from pys_html import build_arg_parser, get_spark, print_args, print_dataframe, print_header
from pys_html.parsing import explode_links, extract_links

SAMPLE_ROWS = [
    ("page1", '<div><a href="/a">Alpha</a><a href="/b">Beta</a></div>'),
    ("page2", '<div><a href="/c">Gamma</a></div>'),
]


def parse_args() -> argparse.Namespace:
    """Define and parse the CLI options accepted by this example."""
    parser = build_arg_parser("Extract <a href> links from an HTML column into a structured column.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--html", type=str, default=None, help="Raw HTML string to scan for links (single row).")
    group.add_argument("--input", "-i", type=str, default=None, help="HTML file to scan for links (single row).")
    return parser.parse_args()


def _resolve_rows(args: argparse.Namespace) -> list[tuple[str, str]]:
    """Return [(page, html)] rows: CLI-provided single row, or the bundled sample rows."""
    if args.html:
        return [("cli", args.html)]
    if args.input:
        return [("cli", Path(args.input).read_text(encoding="utf-8"))]
    return SAMPLE_ROWS


if __name__ == "__main__":
    cli_args = parse_args()
    print_args(cli_args)
    rows = _resolve_rows(cli_args)

    spark = get_spark("extract_links")

    df = spark.createDataFrame(rows, schema="page STRING, html STRING")

    print_header("Raw links column (array<struct<text,href>>)")
    with_links = df.withColumn("links", extract_links("html"))
    print_dataframe(with_links.select("page", "links"))

    print_header("Exploded — one row per link")
    exploded = explode_links(df, html_col="html", link_col="link")
    print_dataframe(exploded.select("page", "link.text", "link.href"))

    spark.stop()
