"""Create a DataFrame directly from a scraped HTML table (end-to-end).

Usage:
    # Uses the bundled product-table sample by default
    uv run python examples/03_dataframe/01_create_dataframe.py

    # Read your own file/URL instead
    uv run python examples/03_dataframe/01_create_dataframe.py --input path/to/page.html
    uv run python examples/03_dataframe/01_create_dataframe.py --url https://example.com/page.html
"""

from pys_html import (
    HtmlReader,
    add_source_args,
    build_arg_parser,
    get_spark,
    print_args,
    print_dataframe,
    print_header,
    resolve_html_source,
)

SAMPLE_HTML = """
<table>
  <tr><th>Product</th><th>Price</th><th>Qty</th></tr>
  <tr><td>Widget</td><td>9.99</td><td>100</td></tr>
  <tr><td>Gadget</td><td>19.99</td><td>50</td></tr>
</table>
"""


def parse_args():
    """Define and parse the CLI options accepted by this example."""
    parser = build_arg_parser("Create a Spark DataFrame directly from a scraped HTML table.")
    add_source_args(parser, input_help="HTML file containing a <table> element.")
    return parser.parse_args()


if __name__ == "__main__":
    cli_args = parse_args()
    print_args(cli_args)
    html_source = resolve_html_source(cli_args, SAMPLE_HTML, "product_catalog.html")

    spark = get_spark("create_dataframe")

    df = HtmlReader(spark).read_table(html_source)

    print_header("Raw table DataFrame")
    print_dataframe(df)
    df.printSchema()

    spark.stop()
