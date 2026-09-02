"""Multi-tab executive summary report: KPI summary sheet + department & detail sheets.

Key concepts:
    - Executives typically want one workbook with a top-level KPI "Summary"
      tab first, followed by supporting breakdown/detail tabs — not a single
      flat export
    - Aggregate once in Spark (groupBy/agg), collect only the small summary
      rows, and use write_many() so sheet order in the dict controls tab
      order in the output workbook
"""

from pyspark.sql import functions as F

from pys_excel import (
    ExcelReader,
    ExcelWriter,
    generate_sample_workbook,
    get_spark,
    output_path,
    print_header,
    print_path,
    print_success,
    set_log_level,
)
from pys_excel._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.multi_tab_summary_report")


if __name__ == "__main__":
    spark = get_spark("excel-multi-tab-summary-report")

    workbook = generate_sample_workbook()
    sheets = ExcelReader(spark).read_all_sheets(workbook)
    employees, departments = sheets["Employees"], sheets["Departments"]

    print_header("1. Build the department breakdown (headcount, avg salary, budget)")
    by_department = (
        employees.groupBy("department")
        .agg(F.count("*").alias("headcount"), F.round(F.avg("salary"), 2).alias("avg_salary"))
        .join(departments, on="department", how="left")
        .orderBy(F.desc("headcount"))
    )
    print_path("Aggregated from", workbook)

    print_header("2. Build a single-row executive KPI summary")
    totals = employees.agg(
        F.count("*").alias("total_headcount"),
        F.round(F.sum("salary"), 2).alias("total_salary_spend"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    ).first()
    top_department = by_department.first()
    summary = spark.createDataFrame(
        [
            (
                int(totals["total_headcount"]),
                float(totals["total_salary_spend"]),
                float(totals["avg_salary"]),
                int(departments.agg(F.sum("budget")).first()[0]),
                top_department["department"],
                int(top_department["headcount"]),
            )
        ],
        schema="total_headcount int, total_salary_spend double, avg_salary double, "
        "total_budget long, top_department string, top_department_headcount long",
    )

    print_header("3. Write the report with Summary as the first tab")
    report_path = output_path("executive_summary_report.xlsx")
    ExcelWriter().write_many(
        {
            "Summary": summary,
            "By Department": by_department,
            "Employees": employees,
        },
        report_path,
    )
    print_path("Executive report", report_path)
    print_success("Multi-tab report written (Summary, By Department, Employees)")

    spark.stop()
