"""
Generate PDF fixture files used by the spark-pdf examples and tests.

Run once before running the examples or the test suite:
    uv run python tests/fixtures/generate_fixtures.py

Produces:
    tests/fixtures/text_article.pdf   - 4-page text article
    tests/fixtures/invoice.pdf        - single-page invoice with a table
    tests/fixtures/sales_report.pdf   - 3-page regional sales report
    tests/fixtures/multi/doc_1.pdf    - 1-page mini document (for glob loading)
    tests/fixtures/multi/doc_2.pdf
    tests/fixtures/multi/doc_3.pdf
"""

from __future__ import annotations

import os
from pathlib import Path

from fpdf import FPDF
from fpdf.enums import XPos, YPos

FIXTURES = Path(__file__).parent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _new(orientation: str = "P") -> FPDF:
    pdf = FPDF(orientation=orientation, unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)
    return pdf


def _heading(pdf: FPDF, text: str, size: int = 14) -> None:
    pdf.set_font("Helvetica", style="B", size=size)
    pdf.multi_cell(0, 8, text)
    pdf.ln(2)


def _body(pdf: FPDF, text: str, size: int = 11) -> None:
    pdf.set_font("Helvetica", size=size)
    pdf.multi_cell(0, 6, text)
    pdf.ln(3)


# ---------------------------------------------------------------------------
# text_article.pdf  (4 pages - rich paragraphs for text extraction)
# ---------------------------------------------------------------------------

ARTICLE_PAGES = [
    (
        "Introduction to Distributed Data Processing",
        (
            "Distributed data processing has transformed the way organisations handle "
            "large-scale analytics. By splitting work across many nodes, frameworks such "
            "as Apache Spark can process terabytes of data in minutes that would take "
            "hours on a single machine.\n\n"
            "This article introduces the core concepts: resilient distributed datasets "
            "(RDDs), DataFrames, and the Catalyst query optimiser, and explains how they "
            "work together to deliver high-throughput, fault-tolerant computation."
        ),
    ),
    (
        "Apache Spark Architecture",
        (
            "A Spark application consists of a driver process and a set of executor "
            "processes. The driver coordinates the job by constructing a directed acyclic "
            "graph (DAG) of stages, while executors perform the actual data transformations.\n\n"
            "The cluster manager (YARN, Kubernetes, or Spark's built-in standalone mode) "
            "allocates resources. Executors cache intermediate data in memory, which is "
            "the key reason Spark outperforms Hadoop MapReduce for iterative workloads."
        ),
    ),
    (
        "DataFrames and the Catalyst Optimiser",
        (
            "The DataFrame API provides a schema-aware abstraction over RDDs. Queries are "
            "expressed using high-level operations (filter, groupBy, join) that Catalyst "
            "compiles into an optimised physical plan.\n\n"
            "Catalyst applies rule-based and cost-based optimisations: predicate push-down "
            "moves filters as close to the data source as possible; column pruning removes "
            "unused fields before reading; and join reordering minimises intermediate "
            "data sizes."
        ),
    ),
    (
        "Conclusion and Further Reading",
        (
            "Apache Spark continues to evolve rapidly. Spark 3.x introduced Adaptive Query "
            "Execution (AQE), which re-optimises query plans at runtime based on actual "
            "partition statistics, dramatically reducing skew and over-partitioning.\n\n"
            "For further reading consult the official documentation at spark.apache.org, "
            "or explore the Learning Spark book (2nd edition, O'Reilly) for end-to-end "
            "production examples."
        ),
    ),
]


def make_text_article(path: Path) -> None:
    pdf = _new()
    for title, body in ARTICLE_PAGES:
        pdf.add_page()
        _heading(pdf, title, size=16)
        _body(pdf, body)
    pdf.output(str(path))
    print(f"  wrote {path.name}  ({len(ARTICLE_PAGES)} pages)")


# ---------------------------------------------------------------------------
# invoice.pdf  (1 page - structured invoice table)
# ---------------------------------------------------------------------------

INVOICE_ITEMS = [
    ("Spark Cluster - 100 node-hours", 1, 4_500.00),
    ("Data Engineering Consulting (5 days)", 5, 1_800.00),
    ("Storage - 10 TB / month", 10, 45.00),
    ("Support SLA - Enterprise tier", 1, 2_200.00),
]


def make_invoice(path: Path) -> None:
    pdf = _new()
    pdf.add_page()

    _heading(pdf, "INVOICE", size=20)
    pdf.set_font("Helvetica", size=10)
    pdf.cell(0, 6, "Invoice #: INV-2025-0042", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(0, 6, "Date: 2025-03-31", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(0, 6, "Bill To: Data Platform Team, Acme Corp", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(6)

    # Table header
    pdf.set_font("Helvetica", style="B", size=10)
    pdf.set_fill_color(220, 220, 220)
    col_w = [90, 20, 30, 35]
    headers = ["Description", "Qty", "Unit Price", "Total"]
    for w, h in zip(col_w, headers):
        pdf.cell(w, 8, h, border=1, fill=True)
    pdf.ln()

    # Table rows
    pdf.set_font("Helvetica", size=10)
    grand_total = 0.0
    for desc, qty, unit in INVOICE_ITEMS:
        total = qty * unit
        grand_total += total
        row = [desc, str(qty), f"${unit:,.2f}", f"${total:,.2f}"]
        for w, val in zip(col_w, row):
            pdf.cell(w, 7, val, border=1)
        pdf.ln()

    pdf.ln(3)
    pdf.set_font("Helvetica", style="B", size=11)
    pdf.cell(sum(col_w[:3]), 8, "Grand Total", border=1)
    pdf.cell(col_w[3], 8, f"${grand_total:,.2f}", border=1)
    pdf.ln(10)

    _body(pdf, "Payment due within 30 days. Thank you for your business.")
    pdf.output(str(path))
    print(f"  wrote {path.name}  (1 page)")


# ---------------------------------------------------------------------------
# sales_report.pdf  (3 pages - one region per page)
# ---------------------------------------------------------------------------

REGIONS = [
    (
        "North America",
        [
            ("Q1 2025", "New York",     125_400.00, 98_200.00),
            ("Q1 2025", "Los Angeles",   87_300.00, 71_000.00),
            ("Q1 2025", "Chicago",       54_800.00, 44_900.00),
            ("Q2 2025", "New York",     138_700.00, 105_600.00),
            ("Q2 2025", "Los Angeles",   92_100.00, 76_400.00),
        ],
    ),
    (
        "Europe",
        [
            ("Q1 2025", "London",       112_000.00, 90_500.00),
            ("Q1 2025", "Berlin",        67_400.00, 55_100.00),
            ("Q1 2025", "Paris",         78_900.00, 63_200.00),
            ("Q2 2025", "London",       121_500.00, 97_800.00),
            ("Q2 2025", "Berlin",        72_300.00, 58_700.00),
        ],
    ),
    (
        "Asia Pacific",
        [
            ("Q1 2025", "Tokyo",        143_200.00, 118_400.00),
            ("Q1 2025", "Singapore",     96_700.00,  80_100.00),
            ("Q1 2025", "Sydney",        61_500.00,  50_300.00),
            ("Q2 2025", "Tokyo",        158_400.00, 129_600.00),
            ("Q2 2025", "Singapore",    104_300.00,  86_500.00),
        ],
    ),
]


def make_sales_report(path: Path) -> None:
    pdf = _new()
    col_w = [25, 40, 38, 38, 35]
    headers = ["Quarter", "City", "Revenue ($)", "Cost ($)", "Profit ($)"]

    for region, rows in REGIONS:
        pdf.add_page()
        _heading(pdf, f"Regional Sales Report - {region}", size=14)
        _body(pdf, "Figures are in USD. Profit = Revenue - Cost.", size=9)

        pdf.set_font("Helvetica", style="B", size=9)
        pdf.set_fill_color(200, 230, 255)
        for w, h in zip(col_w, headers):
            pdf.cell(w, 7, h, border=1, fill=True)
        pdf.ln()

        pdf.set_font("Helvetica", size=9)
        for quarter, city, rev, cost in rows:
            profit = rev - cost
            for w, val in zip(col_w, [quarter, city, f"{rev:,.0f}", f"{cost:,.0f}", f"{profit:,.0f}"]):
                pdf.cell(w, 6, val, border=1)
            pdf.ln()

        total_rev = sum(r[2] for r in rows)
        total_cost = sum(r[3] for r in rows)
        pdf.set_font("Helvetica", style="B", size=9)
        pdf.cell(sum(col_w[:2]), 6, "Total", border=1)
        pdf.cell(col_w[2], 6, f"{total_rev:,.0f}", border=1)
        pdf.cell(col_w[3], 6, f"{total_cost:,.0f}", border=1)
        pdf.cell(col_w[4], 6, f"{total_rev - total_cost:,.0f}", border=1)
        pdf.ln()

    pdf.output(str(path))
    print(f"  wrote {path.name}  ({len(REGIONS)} pages)")


# ---------------------------------------------------------------------------
# multi/doc_N.pdf  (1 page each - for multi-file glob example)
# ---------------------------------------------------------------------------

DOCS = [
    ("Machine Learning Pipeline Overview",
     "This document describes the end-to-end ML pipeline: data ingestion, feature "
     "engineering, model training, evaluation, and deployment to production."),
    ("Data Governance Policy v2.1",
     "All personally identifiable information (PII) must be encrypted at rest and "
     "in transit. Data retention periods are defined per data classification tier."),
    ("Spark Tuning Checklist",
     "1. Set spark.sql.shuffle.partitions to 2-4x the number of cores.\n"
     "2. Enable AQE (spark.sql.adaptive.enabled=true).\n"
     "3. Use Parquet with Snappy compression for large datasets.\n"
     "4. Avoid collect() on large DataFrames; use show() or write instead."),
]


def make_multi_docs(base: Path) -> None:
    base.mkdir(parents=True, exist_ok=True)
    for i, (title, body) in enumerate(DOCS, start=1):
        pdf = _new()
        pdf.add_page()
        _heading(pdf, title)
        _body(pdf, body)
        out = base / f"doc_{i}.pdf"
        pdf.output(str(out))
        print(f"  wrote multi/{out.name}  (1 page)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Generating PDF fixtures …")
    make_text_article(FIXTURES / "text_article.pdf")
    make_invoice(FIXTURES / "invoice.pdf")
    make_sales_report(FIXTURES / "sales_report.pdf")
    make_multi_docs(FIXTURES / "multi")
    print("Done.")
