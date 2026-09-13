"""Merge the *matching* table from multiple HTML files into a single Spark DataFrame.

A common scenario: several report files each contain the same table (e.g. `id="results"`)
but with different rows — one file per day/branch/region. This example targets that table
in every file with `.attrs()`/`.match()`/`.header()` and unions the matches together, tagging
each row with its source file.

Usage:
    # Uses two bundled sample reports (same "results" table shape, different rows) by default
    uv run python examples/01_data_source/06_merge_matching_tables.py

    # Merge your own files
    uv run python examples/01_data_source/06_merge_matching_tables.py \\
        --inputs report_jan.html report_feb.html report_mar.html --attrs id=results

    # Merge every file matched by a glob
    uv run python examples/01_data_source/06_merge_matching_tables.py \\
        --input-glob "data/reports/*.html" --attrs id=results
"""

from pys_html import (
    HtmlReader,
    add_multi_source_args,
    add_parser_arg,
    build_arg_parser,
    get_spark,
    parse_attrs,
    print_args,
    print_dataframe,
    print_header,
    resolve_html_sources,
)

SAMPLE_JAN = """
<html><body>
<table id="unrelated"><tr><th>Ignore</th></tr><tr><td>Me</td></tr></table>
<table id="results">
  <tr><th>Region</th><th>Month</th><th>Revenue</th></tr>
  <tr><td>East</td><td>Jan</td><td>1000</td></tr>
  <tr><td>West</td><td>Jan</td><td>1200</td></tr>
</table>
</body></html>
"""

SAMPLE_FEB = """
<html><body>
<table id="results">
  <tr><th>Region</th><th>Month</th><th>Revenue</th></tr>
  <tr><td>East</td><td>Feb</td><td>1100</td></tr>
  <tr><td>West</td><td>Feb</td><td>1250</td></tr>
</table>
</body></html>
"""


def parse_args():
    """Define and parse the CLI options accepted by this example."""
    parser = build_arg_parser("Merge the matching <table> from multiple HTML files/URLs into one DataFrame.")
    add_multi_source_args(parser)
    add_parser_arg(parser)
    parser.add_argument(
        "--attrs", type=str, default="id=results", help="KEY=VALUE attribute filter for the target table."
    )
    parser.add_argument("--match", type=str, default=None, help="Only keep tables whose text matches this regex.")
    parser.add_argument("--header", type=int, default=0, help="Row index to use as the column header.")
    parser.add_argument(
        "--source-column",
        type=str,
        default="_source_file",
        help="Name of the column recording which source each row came from.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    cli_args = parse_args()
    print_args(cli_args)

    sources = resolve_html_sources(
        cli_args, [("merge_sample_jan.html", SAMPLE_JAN), ("merge_sample_feb.html", SAMPLE_FEB)]
    )

    spark = get_spark("merge_matching_tables")

    reader = HtmlReader(spark).with_parser(cli_args.parser).header(cli_args.header)
    if cli_args.attrs:
        reader = reader.attrs(parse_attrs(cli_args.attrs) or {})
    if cli_args.match:
        reader = reader.match(cli_args.match)

    merged_df = reader.read_matching_tables(sources, source_column=cli_args.source_column)

    print_header(f"Merged {len(sources)} source(s) into {merged_df.count()} row(s)")
    print_dataframe(merged_df)

    spark.stop()
