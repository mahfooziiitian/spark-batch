"""List every <table> element across one or more HTML documents, with attributes and shape.

Useful for discovering which table to target — e.g. find the `id`/`class` to pass to
`--attrs` in the other 01_data_source examples — without reading the full table data.

Usage:
    # Uses the bundled sample (two tables, one unrelated + one "results") by default
    uv run python examples/01_data_source/05_list_tables_and_attrs.py

    # Inspect a single file or URL
    uv run python examples/01_data_source/05_list_tables_and_attrs.py --input path/to/page.html
    uv run python examples/01_data_source/05_list_tables_and_attrs.py --url https://example.com/page.html

    # Inspect every file matched by a glob (e.g. a whole directory of reports)
    uv run python examples/01_data_source/05_list_tables_and_attrs.py --input-glob "data/reports/*.html"

    # Inspect several explicit files
    uv run python examples/01_data_source/05_list_tables_and_attrs.py --inputs a.html b.html c.html
"""

from rich.table import Table

from pys_html import (
    HtmlReader,
    add_multi_source_args,
    add_source_args,
    build_arg_parser,
    get_spark,
    print_args,
    print_header,
    resolve_html_source,
    resolve_html_sources,
)
from pys_html._logging import console

SAMPLE_HTML = """
<html><body>
<table id="unrelated" class="ignore-me">
  <tr><th>Ignore</th></tr>
  <tr><td>Me</td></tr>
</table>
<table id="results" class="data-table striped">
  <caption>Programming Language Popularity</caption>
  <tr><th>Rank</th><th>Language</th><th>Popularity</th></tr>
  <tr><td>1</td><td>Python</td><td>29.9%</td></tr>
  <tr><td>2</td><td>Java</td><td>16.4%</td></tr>
  <tr><td>3</td><td>C++</td><td>11.3%</td></tr>
</table>
</body></html>
"""


def parse_args():
    """Define and parse the CLI options accepted by this example."""
    parser = build_arg_parser("List every <table> element across one or more HTML documents.")
    add_source_args(parser, input_help="Single HTML file to inspect.")
    add_multi_source_args(parser)
    return parser.parse_args()


def _resolve_sources(cli_args) -> list[str]:
    """Resolve the list of sources to inspect: --input/--url, --inputs/--input-glob, or sample."""
    if getattr(cli_args, "input", None) or getattr(cli_args, "url", None):
        return [resolve_html_source(cli_args, SAMPLE_HTML, "tables_attrs_listing.html")]
    return resolve_html_sources(cli_args, [("tables_attrs_listing.html", SAMPLE_HTML)])


def _print_table_summaries(source: str, summaries: list[dict[str, object]]) -> None:
    """Pretty-print list_tables() results for one source as a Rich table."""
    table = Table(title=f"Tables found in {source}", show_lines=True, header_style="header", expand=True)
    table.add_column("Index", style="cyan", justify="right", ratio=1, no_wrap=True)
    table.add_column("Attributes", style="magenta", overflow="fold", ratio=3)
    table.add_column("Rows", justify="right", ratio=1, no_wrap=True)
    table.add_column("Cols", justify="right", ratio=1, no_wrap=True)
    table.add_column("Headers", overflow="fold", ratio=3)
    table.add_column("Caption", overflow="fold", ratio=2)

    for summary in summaries:
        attrs = summary["attrs"]
        attrs_str = ", ".join(f'{k}="{v}"' for k, v in attrs.items()) if attrs else "[dim]<none>[/dim]"
        headers = summary["headers"]
        headers_str = ", ".join(headers) if headers else "[dim]<none>[/dim]"
        table.add_row(
            str(summary["index"]),
            attrs_str,
            str(summary["num_rows"]),
            str(summary["num_cols"]),
            headers_str,
            str(summary["caption"]) if summary["caption"] else "[dim]<none>[/dim]",
        )

    console.print(table)


if __name__ == "__main__":
    cli_args = parse_args()
    print_args(cli_args)
    sources = _resolve_sources(cli_args)

    spark = get_spark("list_tables_and_attrs")

    reader = HtmlReader(spark)
    total_tables = 0
    for source in sources:
        summaries = reader.list_tables(source)
        total_tables += len(summaries)
        print_header(f"Found {len(summaries)} <table> element(s) in: {source}")
        _print_table_summaries(source, summaries)

    print_header(f"Total: {total_tables} table(s) across {len(sources)} source(s)")

    spark.stop()
