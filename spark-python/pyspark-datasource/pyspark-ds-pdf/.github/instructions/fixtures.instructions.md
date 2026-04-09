---
applyTo: "tests/fixtures/**"
---

# PDF Fixture Conventions

## Purpose

The `tests/fixtures/` directory contains pre-generated PDF files used by tests
and examples.  All PDFs are produced by `generate_fixtures.py` using `fpdf2`.

## Regenerating Fixtures

Run once before executing tests or examples:

```bash
uv run python tests/fixtures/generate_fixtures.py
```

## Fixture Catalogue

| File | Pages | Content |
|------|-------|---------|
| `sample.pdf` | multi | Generic reference PDF downloaded from Mozilla PDF.js |
| `text_article.pdf` | 4 | Multi-section article — intro, Spark architecture, Catalyst, conclusion |
| `invoice.pdf` | 1 | Invoice with line items, quantities, unit prices, grand total |
| `sales_report.pdf` | 3 | Regional sales table — North America, Europe, Asia Pacific |
| `multi/doc_1.pdf` | 1 | ML pipeline overview |
| `multi/doc_2.pdf` | 1 | Data governance policy |
| `multi/doc_3.pdf` | 1 | Spark tuning checklist |

## fpdf2 Conventions

### FPDF instance

```python
from fpdf import FPDF
from fpdf.enums import XPos, YPos

def _new(orientation: str = "P") -> FPDF:
    pdf = FPDF(orientation=orientation, unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)
    return pdf
```

### Headings and body text

```python
def _heading(pdf: FPDF, text: str, size: int = 14) -> None:
    pdf.set_font("Helvetica", style="B", size=size)
    pdf.multi_cell(0, 8, text)
    pdf.ln(2)

def _body(pdf: FPDF, text: str, size: int = 11) -> None:
    pdf.set_font("Helvetica", size=size)
    pdf.multi_cell(0, 6, text)
    pdf.ln(3)
```

### Cell with new_x / new_y (fpdf2 ≥ 2.5)

Use `new_x=XPos.LMARGIN, new_y=YPos.NEXT` instead of the deprecated `ln=True`:

```python
pdf.cell(0, 6, "Invoice #: INV-2025-0042", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
```

### Encoding

The built-in Helvetica font only supports **Latin-1** characters.
Do **not** use Unicode dashes (`–` U+2013, `—` U+2014) or other non-Latin-1 characters.
Use plain ASCII hyphens (`-`) instead.

```python
# ✅ Good
"Sales Report - North America"

# ❌ Bad  (raises FPDFUnicodeEncodingException)
"Sales Report – North America"
```

### Table pattern

```python
col_widths = [90, 20, 30, 35]
headers    = ["Description", "Qty", "Unit Price", "Total"]

pdf.set_font("Helvetica", style="B", size=10)
pdf.set_fill_color(220, 220, 220)
for w, h in zip(col_widths, headers):
    pdf.cell(w, 8, h, border=1, fill=True)
pdf.ln()

pdf.set_font("Helvetica", size=10)
for row in data_rows:
    for w, val in zip(col_widths, row):
        pdf.cell(w, 7, val, border=1)
    pdf.ln()
```

### Saving

```python
pdf.output(str(path))
```

## Adding a New Fixture

1. Add a `make_<name>()` function in `generate_fixtures.py`.
2. Call it from the `if __name__ == "__main__":` block.
3. Reference the new path constant in `test_pdf_reader.py`.
4. Add a corresponding test class `Test<Name>Pdf`.
5. Re-run `generate_fixtures.py` to produce the file.

## What NOT to Commit

Generated PDF fixtures should not be committed to version control — add them to
`.gitignore` instead and regenerate from `generate_fixtures.py`:

```gitignore
tests/fixtures/*.pdf
tests/fixtures/multi/*.pdf
!tests/fixtures/sample.pdf   # keep the downloaded reference PDF
```
