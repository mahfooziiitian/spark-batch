"""Generate sample Excel workbooks used by the examples.

Run once before working through the examples/ directory:

    uv run python scripts/generate_sample_data.py
"""

from pys_excel import data_path, generate_sample_workbook
from pys_excel._logging import get_logger, print_path, print_success

logger = get_logger("scripts.generate_sample_data")


def main() -> None:
    path = generate_sample_workbook()
    print_success("Generated sample workbook")
    print_path("Workbook", path)
    print_path("Data home", data_path())


if __name__ == "__main__":
    main()
