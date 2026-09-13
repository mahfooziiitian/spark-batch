"""Strip HTML tags to plain text and extract specific tag content within a DataFrame.

Usage:
    # Uses the bundled two-row sample by default
    uv run python examples/02_parsing/02_extract_text.py

    # Process your own HTML string or file instead (single row)
    uv run python examples/02_parsing/02_extract_text.py --html '<h1>Hi</h1><p>Body</p>'
    uv run python examples/02_parsing/02_extract_text.py --input path/to/page.html

    # Extract a different tag than <h1>
    uv run python examples/02_parsing/02_extract_text.py --tag h2
"""

import argparse
from pathlib import Path

from pys_html import build_arg_parser, get_spark, print_args, print_dataframe, print_header
from pys_html.parsing import extract_tag_text, strip_html_tags

SAMPLE_ROWS = [
    ("article1", "<article><h1>Spark 4</h1><p>Faster joins and <b>ANSI mode</b> by default.</p></article>"),
    ("article2", "<article><h1>PySpark HTML</h1><p>Bridging pandas and BeautifulSoup.</p></article>"),
]


def parse_args() -> argparse.Namespace:
    """Define and parse the CLI options accepted by this example."""
    parser = build_arg_parser("Strip HTML tags to plain text and extract specific tag content.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--html", type=str, default=None, help="Raw HTML string to process (single row).")
    group.add_argument("--input", "-i", type=str, default=None, help="HTML file to process (single row).")
    parser.add_argument("--tag", type=str, default="h1", help="HTML tag whose text content should be extracted.")
    return parser.parse_args()


def _resolve_rows(args: argparse.Namespace) -> list[tuple[str, str]]:
    """Return [(id, html)] rows: CLI-provided single row, or the bundled sample rows."""
    if args.html:
        return [("cli", args.html)]
    if args.input:
        return [("cli", Path(args.input).read_text(encoding="utf-8"))]
    return SAMPLE_ROWS


if __name__ == "__main__":
    cli_args = parse_args()
    print_args(cli_args)
    rows = _resolve_rows(cli_args)

    spark = get_spark("extract_text")

    df = spark.createDataFrame(rows, schema="id STRING, html STRING")

    print_header("Plain text (tags stripped)")
    plain = df.withColumn("plain_text", strip_html_tags("html"))
    print_dataframe(plain.select("id", "plain_text"))

    print_header(f"Extracted <{cli_args.tag}> content")
    tagged = df.withColumn("extracted", extract_tag_text("html", cli_args.tag))
    print_dataframe(tagged.select("id", "extracted"))

    spark.stop()
