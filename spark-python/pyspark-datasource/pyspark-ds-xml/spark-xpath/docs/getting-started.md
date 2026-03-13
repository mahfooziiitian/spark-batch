# Getting Started

## Prerequisites

| Requirement | Version | Check Command |
|---|---|---|
| Python | ≥ 3.11 | `python --version` |
| Java (JDK) | 8, 11, or 17 | `java -version` |
| uv *(recommended)* | latest | `uv --version` |

!!! warning "Java is required"
    PySpark launches a **JVM** under the hood. Make sure `java -version` prints
    a valid version before proceeding. Both OpenJDK and Oracle JDK work.

---

## Installation

=== "uv (Recommended)"

    ```bash
    git clone https://github.com/<owner>/<repo>.git
    cd spark-xpath
    uv sync                    # (1)!
    ```

    1.  `uv sync` resolves all dependencies (including PySpark and dev tools)
        from `pyproject.toml` and creates a virtual environment automatically.

=== "pip + venv"

    ```bash
    git clone https://github.com/<owner>/<repo>.git
    cd spark-xpath
    python -m venv .venv
    source .venv/bin/activate  # (1)!
    pip install -e .
    pip install pytest pytest-mock pytest-sugar  # dev deps
    ```

    1.  On Windows use `.venv\Scripts\activate` instead.

---

## Environment Setup

Set these environment variables if needed by your scripts:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk   # (1)!
export DATA_HOME=/path/to/data                   # (2)!
```

1.  Required if PySpark cannot locate Java automatically.
2.  Used by `nested_xml_xpath.py` to locate XML data files.

---

## Verify the Setup

```bash
uv run pytest tests/ -v
```

??? success "Expected output"
    ```
    tests/xml/test_xml_array_xpath.py::test_xpath_string_extracts_header_fields PASSED
    tests/xml/test_xml_array_xpath.py::test_xpath_string_wildcard PASSED
    tests/xml/test_xml_array_xpath.py::test_xpath_array_extraction PASSED
    tests/xml/test_xml_array_xpath.py::test_xpath_boolean PASSED
    tests/xml/test_xml_array_xpath.py::test_xpath_with_namespaced_xml PASSED
    tests/xml/test_xml_array_xpath.py::test_xpath_multiple_rows PASSED

    ========================= 6 passed =========================
    ```

---

## Your First XPath Query

Create a file called `try_xpath.py`:

```python title="try_xpath.py" linenums="1"
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

spark = SparkSession.builder.master("local[*]").appName("try").getOrCreate()

xml = ["<book><title>PySpark Cookbook</title><year>2024</year></book>"]
df = spark.createDataFrame(xml, StringType()).withColumnRenamed("value", "data")
df.createOrReplaceTempView("books")

result = spark.sql("""
    SELECT
        xpath_string(data, 'book/title') AS title,
        xpath_string(data, 'book/year')  AS year
    FROM books
""")
result.show()
```

Run it:

=== "uv"

    ```bash
    uv run python try_xpath.py
    ```

=== "pip"

    ```bash
    python try_xpath.py
    ```

??? success "Expected output"
    ```
    +----------------+----+
    |           title|year|
    +----------------+----+
    |PySpark Cookbook |2024|
    +----------------+----+
    ```

---

## Troubleshooting

??? question "Error: `JAVA_HOME is not set`"
    PySpark requires a JDK. Install one and set the environment variable:

    === "Ubuntu / Debian"

        ```bash
        sudo apt install openjdk-17-jdk
        export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
        ```

    === "macOS (Homebrew)"

        ```bash
        brew install openjdk@17
        export JAVA_HOME=$(brew --prefix openjdk@17)
        ```

??? question "Error: `ModuleNotFoundError: No module named 'pyspark'`"
    Make sure you are running inside the virtual environment:

    ```bash
    uv sync          # creates .venv and installs deps
    uv run python    # runs with the correct environment
    ```

??? question "Spark is very slow to start"
    On first run Spark downloads ivy dependencies and initializes the JVM.
    Subsequent runs are much faster. You can suppress verbose Spark logs by
    setting the log level:

    ```python
    spark.sparkContext.setLogLevel("WARN")
    ```

---

## Next Steps

- :material-function-variant: Browse the [XPath Functions Reference](xpath-functions.md)
- :material-code-braces: Explore [Basic Parsing](examples/basic-parsing.md) examples
- :material-bank: See the [Credit Evaluation](examples/credit-evaluation.md) real-world walkthrough
- :material-test-tube: Learn how to [write tests](testing.md)
