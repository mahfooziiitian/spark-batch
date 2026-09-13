"""Centralized logging configuration for the pys_excel library.

Provides a pre-configured logger with Rich-powered formatting and level control
via the PYS_EXCEL_LOG_LEVEL environment variable.

Features:
    - Rich console handler with syntax highlighting and tracebacks
    - Structured log format with timestamps and module names
    - DataFrame pretty-printing via rich Tables
    - Console singleton for direct rich output in examples
"""

from __future__ import annotations

import logging
import os

from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.theme import Theme

# Custom theme for pys_excel output
_THEME = Theme(
    {
        "info": "cyan",
        "warning": "yellow",
        "error": "bold red",
        "success": "bold green",
        "path": "blue underline",
        "schema": "magenta",
        "header": "bold bright_white on dark_green",
    }
)

# Shared console instance for all library output
console = Console(theme=_THEME, stderr=True)

_LOG_FORMAT = "%(message)s"
_configured = False


def _configure_root_logger() -> None:
    """One-time setup of the pys_excel logger hierarchy with Rich handler."""
    global _configured
    if _configured:
        return

    level_name = os.environ.get("PYS_EXCEL_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    root_logger = logging.getLogger("pys_excel")
    root_logger.setLevel(level)

    if not root_logger.handlers:
        handler = RichHandler(
            console=console,
            show_time=True,
            show_path=False,
            rich_tracebacks=True,
            tracebacks_show_locals=True,
            markup=True,
        )
        handler.setLevel(level)
        root_logger.addHandler(handler)

    root_logger.propagate = False
    _configured = True


def get_logger(name: str) -> logging.Logger:
    """Get a child logger under the pys_excel namespace.

    Args:
        name: Module or component name (e.g., "reader", "writer", "config").

    Returns:
        Configured logger instance with Rich formatting.

    Example:
        >>> from pys_excel._logging import get_logger
        >>> logger = get_logger("reader")
        >>> logger.info("Reading Excel from %s", path)
    """
    _configure_root_logger()
    return logging.getLogger(f"pys_excel.{name}")


def set_log_level(level: str | int) -> None:
    """Dynamically change the log level for all pys_excel loggers.

    Args:
        level: Level name ("DEBUG", "INFO", "WARNING", "ERROR") or logging constant.
    """
    _configure_root_logger()
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)
    root_logger = logging.getLogger("pys_excel")
    root_logger.setLevel(level)
    for handler in root_logger.handlers:
        handler.setLevel(level)


def print_schema(df, title: str = "Schema") -> None:
    """Pretty-print a DataFrame schema using Rich tree/panel.

    Args:
        df: PySpark DataFrame.
        title: Panel title.
    """
    from rich.tree import Tree

    tree = Tree(f"[bold]{title}[/bold]")
    for field in df.schema.fields:
        nullable = "nullable" if field.nullable else "not null"
        tree.add(f"[cyan]{field.name}[/cyan]: [magenta]{field.dataType.simpleString()}[/magenta] ({nullable})")
    console.print(tree)


def print_dataframe(df, title: str = "DataFrame", max_rows: int = 20) -> None:
    """Pretty-print a PySpark DataFrame as a Rich table.

    Args:
        df: PySpark DataFrame.
        title: Table title.
        max_rows: Maximum rows to display.
    """
    table = Table(title=title, show_lines=True, header_style="header")
    for field in df.schema.fields:
        table.add_column(field.name, style="cyan", overflow="fold")

    rows = df.limit(max_rows).collect()
    for row in rows:
        table.add_row(*[str(v) if v is not None else "[dim]null[/dim]" for v in row])

    if df.count() > max_rows:
        table.caption = f"Showing {max_rows} of {df.count()} rows"

    console.print(table)


def print_header(text: str) -> None:
    """Print a section header panel.

    Args:
        text: Header text.
    """
    console.print(Panel(Text(text, justify="center"), style="header", expand=True))


def print_success(message: str) -> None:
    """Print a success message."""
    console.print(f"[success]✓[/success] {message}")


def print_warning(message: str) -> None:
    """Print a warning message."""
    console.print(f"[warning]⚠[/warning] {message}")


def print_error(message: str) -> None:
    """Print an error message."""
    console.print(f"[error]✗[/error] {message}")


def print_path(label: str, path: str) -> None:
    """Print a labeled file path."""
    console.print(f"  {label}: [path]{path}[/path]")
