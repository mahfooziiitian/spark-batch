"""Extract <meta name="..." content="..."> tags into a map column.

Usage:
    # Uses the bundled sample document by default
    uv run python examples/02_parsing/03_extract_meta_tags.py

    # Process your own HTML string or file instead
    uv run python examples/02_parsing/03_extract_meta_tags.py --html '<meta name="a" content="b">'
    uv run python examples/02_parsing/03_extract_meta_tags.py --input path/to/page.html

    # Look up specific meta keys after extraction
    uv run python examples/02_parsing/03_extract_meta_tags.py --keys description,author
"""

import argparse
from pathlib import Path

from pyspark.sql.functions import col

from pys_html import build_arg_parser, get_spark, print_args, print_dataframe, print_header
from pys_html.parsing import extract_meta_tags

SAMPLE_HTML = """
<html><head>
  <meta name="description" content="PySpark HTML datasource examples">
  <meta name="author" content="spark-batch">
  <meta property="og:title" content="PySpark HTML">
</head><body></body></html>
"""


def parse_args() -> argparse.Namespace:
    """Define and parse the CLI options accepted by this example."""
    parser = build_arg_parser("Extract <meta> tags from an HTML column into a map<string,string> column.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--html", type=str, default=None, help="Raw HTML string to process.")
    group.add_argument("--input", "-i", type=str, default=None, help="HTML file to process.")
    parser.add_argument(
        "--keys",
        type=str,
        default="description,author",
        help="Comma-separated meta keys to look up individually after extraction.",
    )
    return parser.parse_args()


def _resolve_html(args: argparse.Namespace) -> str:
    """Return the HTML document to scan: --html, --input, or the bundled sample."""
    if args.html:
        return args.html
    if args.input:
        return Path(args.input).read_text(encoding="utf-8")
    return SAMPLE_HTML


if __name__ == "__main__":
    cli_args = parse_args()
    print_args(cli_args)
    html_doc = _resolve_html(cli_args)
    keys = [k.strip() for k in cli_args.keys.split(",") if k.strip()]

    spark = get_spark("extract_meta_tags")

    df = spark.createDataFrame([("doc1", html_doc)], schema="id STRING, html STRING")

    print_header("Extracted meta tags (map<string,string>)")
    with_meta = df.withColumn("meta", extract_meta_tags("html"))
    print_dataframe(with_meta.select("id", "meta"))

    if keys:
        print_header(f"Individual keys: {', '.join(keys)}")
        lookups = [col("meta")[key].alias(key) for key in keys]
        with_meta.select("id", *lookups).show(truncate=False)

    spark.stop()
