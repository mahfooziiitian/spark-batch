"""Write a Spark DataFrame back out as an HTML table / standalone page.

Usage:
    # Writes to the default output_path() directory
    uv run python examples/03_dataframe/03_write_html.py

    # Customize output directory, CSS classes, border width, and page title
    uv run python examples/03_dataframe/03_write_html.py --output-dir /tmp/html_out \\
        --classes "table table-dark" --border 2 --title "Catalog"
"""

import argparse
from pathlib import Path

from pys_html import (
    HtmlWriter,
    build_arg_parser,
    get_spark,
    output_path,
    print_args,
    print_header,
    print_path,
    print_success,
)


def parse_args() -> argparse.Namespace:
    """Define and parse the CLI options accepted by this example."""
    parser = build_arg_parser("Write a Spark DataFrame as an HTML table fragment and standalone page.")
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory to write HTML files into. Defaults to output_path() under DATA_HOME.",
    )
    parser.add_argument("--classes", type=str, default="table table-striped", help="CSS class(es) for the <table>.")
    parser.add_argument("--border", type=int, default=1, help="HTML table border width in pixels.")
    parser.add_argument("--title", type=str, default="Product Catalog", help="Title for the standalone HTML page.")
    return parser.parse_args()


if __name__ == "__main__":
    cli_args = parse_args()
    print_args(cli_args)

    spark = get_spark("write_html")

    df = spark.createDataFrame(
        [("Widget", 9.99, 100), ("Gadget", 19.99, 50)],
        schema="product STRING, price DOUBLE, qty INT",
    )

    writer = HtmlWriter(classes=cli_args.classes).border(cli_args.border)
    output_dir = Path(cli_args.output_dir) if cli_args.output_dir else Path(output_path())
    output_dir.mkdir(parents=True, exist_ok=True)

    table_path = str(output_dir / "products_table.html")
    writer.write(df, table_path)
    print_success("Wrote raw <table> fragment")
    print_path("Table file", table_path)

    page_path = str(output_dir / "products_page.html")
    writer.write_page(df, page_path, title=cli_args.title)
    print_success("Wrote standalone HTML page")
    print_path("Page file", page_path)

    print_header("Rendered markup preview")
    print(writer.render(df.limit(1)))

    spark.stop()
