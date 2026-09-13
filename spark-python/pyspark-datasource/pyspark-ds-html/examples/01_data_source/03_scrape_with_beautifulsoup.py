"""Scrape arbitrary HTML (links, headings) into a Spark DataFrame via BeautifulSoup.

Unlike read_html_tables.py (which targets <table> elements), this path handles
any tag — useful for scraping navigation links, headings, or article metadata
that isn't laid out as a table.

Usage:
    # Uses the bundled sample nav/heading page by default
    uv run python examples/01_data_source/03_scrape_with_beautifulsoup.py

    # Scrape links/headings from your own file or URL
    uv run python examples/01_data_source/03_scrape_with_beautifulsoup.py --input path/to/page.html
    uv run python examples/01_data_source/03_scrape_with_beautifulsoup.py --url https://example.com

    # Scrape a different tag instead of <h2>
    uv run python examples/01_data_source/03_scrape_with_beautifulsoup.py --tag h1
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
<nav>
  <a href="/docs">Docs</a>
  <a href="/blog">Blog</a>
  <a href="https://github.com/mahfooziiitian/spark-batch">GitHub</a>
</nav>
<h1>Welcome</h1>
<h2>Getting Started</h2>
<h2>Reference</h2>
</body></html>
"""


def parse_args():
    """Define and parse the CLI options accepted by this example."""
    parser = build_arg_parser("Scrape arbitrary HTML tags (links, headings, ...) into a DataFrame.")
    add_source_args(parser)
    add_parser_arg(parser)
    parser.add_argument(
        "--tag",
        type=str,
        default="h2",
        help="HTML tag to scrape in addition to <a> links.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    cli_args = parse_args()
    print_args(cli_args)
    html_source = resolve_html_source(cli_args, SAMPLE_HTML, "nav_headings.html")

    spark = get_spark("scrape_with_beautifulsoup")

    reader = HtmlReader(spark).with_parser(cli_args.parser)

    print_header("Scraped <a> links")
    links_df = reader.read_links(html_source)
    print_dataframe(links_df)

    print_header(f"Scraped <{cli_args.tag}> elements")
    tag_df = reader.read_elements(html_source, cli_args.tag)
    print_dataframe(tag_df)

    print_header("Plain text (tags stripped)")
    print(reader.read_text(html_source))

    spark.stop()
