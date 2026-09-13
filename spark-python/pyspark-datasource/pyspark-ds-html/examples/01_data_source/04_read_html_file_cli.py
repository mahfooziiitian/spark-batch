"""Generic HTML file reader driven entirely by command-line arguments.

Unlike the other 01_data_source examples (which default to a fixed sample),
this script demonstrates a fully generic "point it at any HTML file/URL" pattern:
every HtmlReader option (parser, attrs, match, header, table index) is exposed
as a CLI flag so the same script works against arbitrary input.

Usage:
    # Uses a bundled sample file when --input/--url are omitted
    uv run python examples/01_data_source/04_read_html_file_cli.py

    # Read every <table> from a local file
    uv run python examples/01_data_source/04_read_html_file_cli.py --input path/to/page.html

    # Read every <table> from a URL
    uv run python examples/01_data_source/04_read_html_file_cli.py --url https://example.com/page.html

    # Target one table by id, header row, and index
    uv run python examples/01_data_source/04_read_html_file_cli.py \\
        --input path/to/page.html --attrs id=results --header 0 --table-index 0

    # Choose a different parser backend
    uv run python examples/01_data_source/04_read_html_file_cli.py --parser html5lib
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
<h2>Programming Language Popularity</h2>
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
    parser = build_arg_parser("Read HTML <table> elements from a file or URL into Spark DataFrame(s).")
    add_source_args(parser, input_help="Path to a local HTML file.")
    add_parser_arg(parser)
    parser.add_argument(
        "--attrs",
        type=str,
        default=None,
        metavar="KEY=VALUE",
        help="Only consider <table> elements with this HTML attribute, e.g. id=results.",
    )
    parser.add_argument(
        "--match",
        type=str,
        default=None,
        help="Only consider tables containing this regex/text somewhere in their rows.",
    )
    parser.add_argument(
        "--header",
        type=int,
        default=None,
        help="Row index (0-based) to use as the column header.",
    )
    parser.add_argument(
        "--table-index",
        type=int,
        default=None,
        metavar="N",
        help="Return only the Nth matched table (0-based) instead of every table found.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    cli_args = parse_args()
    print_args(cli_args)
    html_source = resolve_html_source(cli_args, SAMPLE_HTML, "language_popularity.html")

    spark = get_spark("read_html_file_cli")

    reader = HtmlReader(spark).with_parser(cli_args.parser)
    if cli_args.match:
        reader = reader.match(cli_args.match)
    if cli_args.header is not None:
        reader = reader.header(cli_args.header)
    if attrs := parse_attrs(cli_args.attrs):
        reader = reader.attrs(attrs)

    print_header(f"Reading tables from: {html_source}")

    if cli_args.table_index is not None:
        table = reader.read_table(html_source, index=cli_args.table_index)
        print_dataframe(table, title=f"Table {cli_args.table_index}")
    else:
        tables = reader.read_tables(html_source)
        print_header(f"Found {len(tables)} table(s)")
        for i, df in enumerate(tables):
            print_dataframe(df, title=f"Table {i}")

    spark.stop()
