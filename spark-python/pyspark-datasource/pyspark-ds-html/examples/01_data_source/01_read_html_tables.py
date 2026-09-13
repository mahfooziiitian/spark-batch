"""Read HTML <table> elements into Spark DataFrames via pyspark.pandas.read_html().

Spark has no native HTML source, so pyspark.pandas.read_html() (Spark's own
pandas-on-Spark bridge, lxml/html5lib/bs4 backed) parses every <table> in a
document and each result converts directly to Spark via .to_spark().

Usage:
    # Uses the bundled sample (country population table) by default
    uv run python examples/01_data_source/01_read_html_tables.py

    # Read every <table> from a local file or URL instead
    uv run python examples/01_data_source/01_read_html_tables.py --input path/to/page.html
    uv run python examples/01_data_source/01_read_html_tables.py --url https://example.com/page.html

    # Choose a different parser backend
    uv run python examples/01_data_source/01_read_html_tables.py --parser html5lib
"""

from pys_html import (
    HtmlReader,
    add_parser_arg,
    add_source_args,
    build_arg_parser,
    get_spark,
    print_args,
    print_dataframe,
    print_header,
    resolve_html_source,
)

SAMPLE_HTML = """
<html><body>
<h2>Country Population</h2>
<table id="population">
  <tr><th>Country</th><th>Population</th><th>Continent</th></tr>
  <tr><td>India</td><td>1428627663</td><td>Asia</td></tr>
  <tr><td>USA</td><td>339996563</td><td>North America</td></tr>
  <tr><td>Nigeria</td><td>223804632</td><td>Africa</td></tr>
</table>
</body></html>
"""


def parse_args():
    """Define and parse the CLI options accepted by this example."""
    parser = build_arg_parser("Read every HTML <table> element into a Spark DataFrame.")
    add_source_args(parser, input_help="HTML file containing <table> elements.")
    add_parser_arg(parser)
    return parser.parse_args()


if __name__ == "__main__":
    cli_args = parse_args()
    print_args(cli_args)
    html_source = resolve_html_source(cli_args, SAMPLE_HTML, "country_population.html")

    spark = get_spark("read_html_tables")

    reader = HtmlReader(spark).with_parser(cli_args.parser)
    tables = reader.read_tables(html_source)

    print_header(f"Found {len(tables)} table(s)")
    for i, df in enumerate(tables):
        print_dataframe(df, title=f"Table {i}", max_rows=500)
        df.printSchema()

    spark.stop()
