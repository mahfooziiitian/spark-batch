"""Cast and transform columns extracted from an HTML table (all columns arrive as strings).

Usage:
    # Uses the bundled product-table sample (Product/Price/Qty) by default
    uv run python examples/03_dataframe/02_transformations.py

    # Read your own file/URL — override column names if they differ
    uv run python examples/03_dataframe/02_transformations.py --input path/to/page.html \\
        --price-col Cost --qty-col Quantity
"""

from pyspark.sql.functions import col
from pyspark.sql.functions import round as spark_round

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
    parser = build_arg_parser("Cast HTML table string columns to numeric types and derive new columns.")
    add_source_args(parser, input_help="HTML file containing a <table> element.")
    parser.add_argument("--price-col", type=str, default="Price", help="Name of the price column to cast to double.")
    parser.add_argument("--qty-col", type=str, default="Qty", help="Name of the quantity column to cast to int.")
    return parser.parse_args()


if __name__ == "__main__":
    cli_args = parse_args()
    print_args(cli_args)
    html_source = resolve_html_source(cli_args, SAMPLE_HTML, "product_catalog_transform.html")

    spark = get_spark("transformations")

    raw_df = HtmlReader(spark).read_table(html_source)

    price_col, qty_col = cli_args.price_col, cli_args.qty_col
    typed_df = (
        raw_df.withColumn(price_col, col(price_col).cast("double"))
        .withColumn(qty_col, col(qty_col).cast("int"))
        .withColumn("Total", spark_round(col(price_col) * col(qty_col), 2))
    )

    print_header("Typed + derived columns")
    print_dataframe(typed_df)
    typed_df.printSchema()

    spark.stop()
