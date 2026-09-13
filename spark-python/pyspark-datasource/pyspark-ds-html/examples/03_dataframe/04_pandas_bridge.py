"""Bridge patterns between Spark, pandas, and BeautifulSoup for HTML workloads.

Demonstrates the full round trip: Spark DataFrame -> pandas -> HTML -> pandas
(read_html) -> Spark DataFrame, useful when validating writer output or when
downstream systems only accept small HTML report files.

Usage:
    # Uses the bundled country/population sample by default
    uv run python examples/03_dataframe/04_pandas_bridge.py

    # Customize the writer output (parser flavor used on the read-back side)
    uv run python examples/03_dataframe/04_pandas_bridge.py --parser html5lib --border 2
"""

import argparse

from pys_html import (
    HtmlReader,
    HtmlWriter,
    add_parser_arg,
    build_arg_parser,
    get_spark,
    print_args,
    print_dataframe,
    print_header,
)


def parse_args() -> argparse.Namespace:
    """Define and parse the CLI options accepted by this example."""
    parser = build_arg_parser("Round-trip a Spark DataFrame through HTML (Spark -> HTML -> Spark).")
    add_parser_arg(parser)
    parser.add_argument("--border", type=int, default=0, help="HTML table border width in pixels.")
    return parser.parse_args()


if __name__ == "__main__":
    cli_args = parse_args()
    print_args(cli_args)

    spark = get_spark("pandas_bridge")

    original_df = spark.createDataFrame(
        [("India", 1428627663), ("USA", 339996563), ("Nigeria", 223804632)],
        schema="country STRING, population LONG",
    )

    print_header("Original Spark DataFrame")
    print_dataframe(original_df)

    html_markup = HtmlWriter().border(cli_args.border).render(original_df)

    print_header("Rendered HTML fragment")
    print(html_markup)

    round_tripped_df = HtmlReader(spark).with_parser(cli_args.parser).read_table(html_markup)

    print_header("Round-tripped DataFrame (Spark -> HTML -> Spark)")
    print_dataframe(round_tripped_df)

    spark.stop()
