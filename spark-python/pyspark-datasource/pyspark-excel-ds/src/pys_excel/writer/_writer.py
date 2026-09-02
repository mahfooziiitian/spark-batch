"""Reusable Excel writer with configurable options.

Like the reader, this writer bridges through pandas (``DataFrame.toPandas()``)
and writes with ``pandas.ExcelWriter`` (``xlsxwriter`` engine by default) so
that no JVM Excel package is required for local development. For
cluster-scale writes, see :mod:`pys_excel.spark_excel` which wraps the
``spark-excel`` (crealytics) data source instead of collecting to the driver.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pys_excel._logging import get_logger

if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import DataFrame

logger = get_logger("writer")

# Supported pandas Excel engines
EXCEL_ENGINES = ("xlsxwriter", "openpyxl")


@dataclass
class ExcelWriter:
    """Configurable Excel file writer wrapping ``pandas.ExcelWriter``.

    .. warning::
        This writer calls ``DataFrame.toPandas()``, which collects the full
        result to the driver. It is intended for small-to-medium reporting
        outputs (dashboards, extracts), not large-scale exports. Use
        :mod:`pys_excel.spark_excel` for distributed writes on a cluster.

    Args:
        sheet_name: Target worksheet name.
        index: Whether to include the pandas row index as a column.
        engine: pandas Excel engine (``xlsxwriter`` or ``openpyxl``).
        freeze_header: Freeze the header row when writing.
        autofit_columns: Auto-size column widths to fit content.
        options: Additional keyword args forwarded to ``DataFrame.to_excel``.

    Example:
        >>> writer = ExcelWriter(sheet_name="Employees").autofit_columns()
        >>> writer.write(df, "/output/employees.xlsx")
    """

    sheet_name: str = "Sheet1"
    index: bool = False
    engine: str = "xlsxwriter"
    freeze_header: bool = True
    autofit_columns: bool = True
    options: dict[str, Any] = field(default_factory=dict)

    def write(self, df: DataFrame, path: str) -> None:
        """Write a single-sheet Excel workbook from a Spark DataFrame.

        Args:
            df: DataFrame to write (collected to the driver via toPandas()).
            path: Output .xlsx file path.
        """
        logger.info("Writing Excel to %s (sheet=%s, engine=%s)", path, self.sheet_name, self.engine)
        pdf: pd.DataFrame = df.toPandas()  # type: ignore[assignment]

        with self._excel_writer(path) as writer:
            pdf.to_excel(writer, sheet_name=self.sheet_name, index=self.index, **self.options)
            self._apply_formatting(writer, pdf)
        logger.debug("Write complete: %s", path)

    def write_many(self, sheets: dict[str, DataFrame], path: str) -> None:
        """Write multiple DataFrames as separate sheets in one workbook.

        Args:
            sheets: Mapping of sheet name to DataFrame.
            path: Output .xlsx file path.
        """
        logger.info("Writing %d sheets to %s", len(sheets), path)
        with self._excel_writer(path) as writer:
            for sheet_name, df in sheets.items():
                pdf: pd.DataFrame = df.toPandas()  # type: ignore[assignment]
                pdf.to_excel(writer, sheet_name=sheet_name, index=self.index, **self.options)
                self._apply_formatting(writer, pdf, sheet_name=sheet_name)
        logger.debug("Write complete: %s", path)

    def _excel_writer(self, path: str):
        from pathlib import Path

        import pandas as pd

        Path(path).parent.mkdir(parents=True, exist_ok=True)
        return pd.ExcelWriter(path, engine=self.engine)

    def _apply_formatting(self, writer, pdf, sheet_name: str | None = None) -> None:
        """Apply freeze panes / autofit when using the xlsxwriter engine."""
        if self.engine != "xlsxwriter":
            return
        name = sheet_name or self.sheet_name
        worksheet = writer.sheets[name]

        if self.freeze_header:
            worksheet.freeze_panes(1, 0)

        if self.autofit_columns:
            for i, column in enumerate(pdf.columns):
                width = max(len(str(column)), *(pdf[column].astype(str).map(len).tolist() or [0])) + 2
                worksheet.set_column(i, i, width)

    # --- Fluent modifiers (immutable) ---

    def with_option(self, key: str, value: Any) -> ExcelWriter:
        """Return a new writer with an additional ``to_excel`` option set."""
        new_options = {**self.options, key: value}
        return ExcelWriter(
            sheet_name=self.sheet_name,
            index=self.index,
            engine=self.engine,
            freeze_header=self.freeze_header,
            autofit_columns=self.autofit_columns,
            options=new_options,
        )

    def with_sheet_name(self, name: str) -> ExcelWriter:
        """Return a new writer targeting the given sheet name."""
        return ExcelWriter(
            sheet_name=name,
            index=self.index,
            engine=self.engine,
            freeze_header=self.freeze_header,
            autofit_columns=self.autofit_columns,
            options=self.options,
        )

    def with_index(self, enabled: bool = True) -> ExcelWriter:
        """Return a new writer that includes/excludes the row index column."""
        return ExcelWriter(
            sheet_name=self.sheet_name,
            index=enabled,
            engine=self.engine,
            freeze_header=self.freeze_header,
            autofit_columns=self.autofit_columns,
            options=self.options,
        )

    def with_engine(self, engine: str) -> ExcelWriter:
        """Return a new writer using the given pandas Excel engine."""
        if engine not in EXCEL_ENGINES:
            msg = f"Unsupported engine '{engine}'. Use one of: {EXCEL_ENGINES}"
            logger.error(msg)
            raise ValueError(msg)
        return ExcelWriter(
            sheet_name=self.sheet_name,
            index=self.index,
            engine=engine,
            freeze_header=self.freeze_header,
            autofit_columns=self.autofit_columns,
            options=self.options,
        )

    def with_freeze_header(self, enabled: bool = True) -> ExcelWriter:
        """Return a new writer that toggles header-row freezing (xlsxwriter only)."""
        return ExcelWriter(
            sheet_name=self.sheet_name,
            index=self.index,
            engine=self.engine,
            freeze_header=enabled,
            autofit_columns=self.autofit_columns,
            options=self.options,
        )

    def with_autofit_columns(self, enabled: bool = True) -> ExcelWriter:
        """Return a new writer that toggles column auto-sizing (xlsxwriter only)."""
        return ExcelWriter(
            sheet_name=self.sheet_name,
            index=self.index,
            engine=self.engine,
            freeze_header=self.freeze_header,
            autofit_columns=enabled,
            options=self.options,
        )

    # --- Formatting presets ---

    def date_format(self, pattern: str) -> ExcelWriter:
        """Set the output date format (e.g. "yyyy-mm-dd")."""
        return ExcelWriter(
            sheet_name=self.sheet_name,
            index=self.index,
            engine=self.engine,
            freeze_header=self.freeze_header,
            autofit_columns=self.autofit_columns,
            options={**self.options, "engine_kwargs": {"options": {"default_date_format": pattern}}},
        )
