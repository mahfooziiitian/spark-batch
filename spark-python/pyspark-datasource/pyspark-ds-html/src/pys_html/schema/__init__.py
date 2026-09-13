"""Schema builders — ready-made StructTypes for links, meta tags, and scraped tables."""

from pys_html.schema._schema import (
    link_schema,
    links_array_schema,
    meta_tags_schema,
    table_row_schema,
    with_source_url,
)

__all__ = [
    "link_schema",
    "links_array_schema",
    "meta_tags_schema",
    "table_row_schema",
    "with_source_url",
]
