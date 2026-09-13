"""Filter and target specific <table> elements using match/attrs/header options.

Usage:
    # Uses the bundled sample (language popularity, id=results) by default
    uv run python examples/01_data_source/02_read_html_targeted_table.py

    # Target a table by attribute in your own file/URL
    uv run python examples/01_data_source/02_read_html_targeted_table.py \\
        --input path/to/page.html --attrs id=results

    # Filter by header row / row-text match instead
    uv run python examples/01_data_source/02_read_html_targeted_table.py --header 0 --match Python
"""

from pys_html import (
    HtmlReader,
    add_parser_arg,
    add_source_args,
    build_arg_parser,
    get_spark,
    parse_attrs,
    print_args,
    print_dataframe,
    print_header,
    resolve_html_source,
)

SAMPLE_HTML = """
<html><body>
<table id="unrelated"><tr><th>Ignore</th></tr><tr><td>Me</td></tr></table>
<table id="results">
  <tr><th>Rank</th><th>Language</th><th>Popularity</th></tr>
  <tr><td>1</td><td>Python</td><td>29.9%</td></tr>
  <tr><td>2</td><td>Java</td><td>16.4%</td></tr>
  <tr><td>3</td><td>C++</td><td>11.3%</td></tr>
</table>
</body></html>
"""


def parse_args():
    """Define and parse the CLI options accepted by this example."""
    parser = build_arg_parser("Target a specific <table> element via attrs/match/header filters.")
    add_source_args(parser)
    add_parser_arg(parser)
    parser.add_argument(
        "--attrs",
        type=str,
        default="id=results",
        metavar="KEY=VALUE",
        help="Only consider tables with this HTML attribute.",
    )
    parser.add_argument(
        "--match",
        type=str,
        default=None,
        help="Only consider tables whose text matches this regex/text.",
    )
    parser.add_argument(
        "--header",
        type=int,
        default=None,
        help="Row index (0-based) to use as the column header.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    cli_args = parse_args()
    print_args(cli_args)
    html_source = resolve_html_source(cli_args, SAMPLE_HTML, "language_popularity_targeted.html")

    spark = get_spark("read_html_targeted")

    reader = HtmlReader(spark).with_parser(cli_args.parser)
    if attrs := parse_attrs(cli_args.attrs):
        reader = reader.attrs(attrs)
    if cli_args.match:
        reader = reader.match(cli_args.match)
    if cli_args.header is not None:
        reader = reader.header(cli_args.header)

    df = reader.read_table(html_source)

    print_header(f"Targeted table ({cli_args.attrs})")
    print_dataframe(df)

    spark.stop()
