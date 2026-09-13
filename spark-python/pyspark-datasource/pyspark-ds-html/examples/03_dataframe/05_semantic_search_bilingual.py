"""Semantic (fuzzy, typo-tolerant) search over a merged DataFrame, with bilingual headers.

Combines two earlier capabilities — merging the matching table across multiple HTML files
(`06_merge_matching_tables.py`) and a lightweight text-similarity search (`pys_html.search`)
— to answer questions like "find rows whose Hindi name column is *close to* this query" across
one or more report files, without a heavy embeddings model or GPU/download requirement.

Column headers are displayed bilingually (e.g. "रैयत का नाम / Tenant Name") purely for the
console output in this example; the underlying DataFrame's actual column names are untouched.

Usage:
    # Uses two bundled bilingual sample reports by default, searching "रैयत का नाम" for "लक्ष्मी"
    uv run python examples/03_dataframe/05_semantic_search_bilingual.py

    # Search your own merged reports
    uv run python examples/03_dataframe/05_semantic_search_bilingual.py \\
        --input-glob "/path/to/reports/*.html" --attrs id=MainContent_gvSearch \\
        --search-column "रैयत का नाम" --query "लक्ष्मी" --top-k 15 --min-score 0.5
"""

from rich.table import Table

from pys_html import (
    HtmlReader,
    add_multi_source_args,
    add_parser_arg,
    add_search_args,
    bilingual_header,
    build_arg_parser,
    get_spark,
    parse_attrs,
    print_args,
    print_header,
    resolve_html_sources,
    semantic_search,
)
from pys_html._logging import console

SAMPLE_JAN = """
<html><body>
<table id="results">
  <tr><th>क्रम संख्या</th><th>रैयत का नाम</th><th>खाता संख्या</th><th>भाग वर्त्तमान</th></tr>
  <tr><td>11503</td><td>रौशन परवीन</td><td>3728</td><td>44</td></tr>
  <tr><td>11504</td><td>लक्ष्मी राय</td><td>1391</td><td>12</td></tr>
  <tr><td>11505</td><td>लक्षण राय</td><td>3102</td><td>10</td></tr>
</table>
</body></html>
"""

SAMPLE_FEB = """
<html><body>
<table id="results">
  <tr><th>क्रम संख्या</th><th>रैयत का नाम</th><th>खाता संख्या</th><th>भाग वर्त्तमान</th></tr>
  <tr><td>11520</td><td>लक्षमिनिया देवी</td><td>1551</td><td>27</td></tr>
  <tr><td>11521</td><td>लक्ष्मी दास</td><td>1854</td><td>17</td></tr>
  <tr><td>11522</td><td>लक्ष्मी ठाकुर</td><td>74</td><td>14</td></tr>
</table>
</body></html>
"""

DEFAULT_SEARCH_COLUMN = "रैयत का नाम"
DEFAULT_QUERY = "लक्ष्मी"


def parse_args():
    """Define and parse the CLI options accepted by this example."""
    parser = build_arg_parser("Fuzzy-search a merged, bilingually-labeled DataFrame extracted from HTML tables.")
    add_multi_source_args(parser)
    add_parser_arg(parser)
    parser.add_argument(
        "--attrs", type=str, default="id=results", help="KEY=VALUE attribute filter for the target table."
    )
    parser.add_argument("--header", type=int, default=0, help="Row index to use as the column header.")
    add_search_args(parser, default_column=DEFAULT_SEARCH_COLUMN, default_query=DEFAULT_QUERY)
    return parser.parse_args()


def _print_bilingual_dataframe(df, title: str, max_rows: int = 20) -> None:
    """Pretty-print a DataFrame as a Rich table with bilingual (Hindi / English) headers."""
    table = Table(title=title, show_lines=True, header_style="header", expand=True)
    for field in df.schema.fields:
        table.add_column(bilingual_header(field.name), style="cyan", overflow="fold", ratio=1)

    rows = df.limit(max_rows).collect()
    for row in rows:
        table.add_row(*[str(v) if v is not None else "[dim]null[/dim]" for v in row])

    total = df.count()
    if total > max_rows:
        table.caption = f"Showing {max_rows} of {total} rows"

    console.print(table)


if __name__ == "__main__":
    cli_args = parse_args()
    print_args(cli_args)

    sources = resolve_html_sources(
        cli_args, [("search_sample_jan.html", SAMPLE_JAN), ("search_sample_feb.html", SAMPLE_FEB)]
    )

    spark = get_spark("semantic_search_bilingual")

    reader = HtmlReader(spark).with_parser(cli_args.parser).header(cli_args.header)
    if cli_args.attrs:
        reader = reader.attrs(parse_attrs(cli_args.attrs) or {})

    merged_df = reader.read_matching_tables(sources)
    print_header(f"Merged {len(sources)} source(s) into {merged_df.count()} row(s)")
    _print_bilingual_dataframe(merged_df, title="Merged DataFrame")

    results_df = semantic_search(
        merged_df,
        column=cli_args.search_column,
        query=cli_args.query,
        top_k=cli_args.top_k,
        min_score=cli_args.min_score,
        scorer=cli_args.scorer,
    )

    print_header(
        f"Top matches for {cli_args.search_column!r} ~ {cli_args.query!r} "
        f"(scorer={cli_args.scorer}, min_score={cli_args.min_score})"
    )
    _print_bilingual_dataframe(results_df, title="Search Results")

    spark.stop()
