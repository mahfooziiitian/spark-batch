# Project Structure

## Layout

```
pyspark-pytest/
├── src/
│   ├── data_processing.py              ← Transaction classification pipeline
│   ├── reader/
│   │   └── spark_reader.py             ← CSV reader utility
│   ├── transformation/
│   │   └── df_transformation.py        ← Text transformations
│   └── utility/
│       ├── faker_customized_data.py    ← Custom Faker data
│       ├── faker_locale_data.py        ← Locale-aware Faker data
│       ├── generate_csv_faker_data.py  ← CSV output generator
│       └── generate_faker_data.py      ← JSON output generator
├── tests/
│   ├── conftest.py                     ← Shared SparkSession fixture
│   ├── test_data_processing.py         ← Pipeline tests
│   ├── dataframe/
│   │   ├── test_dataframe.py           ← DataFrame + SQL tests
│   │   └── test_df_equality.py         ← assertDataFrameEqual tests
│   ├── reader/
│   │   └── test_spark_reader.py        ← Mock-based reader tests
│   └── transformation/
│       └── test_df_transformation.py   ← Transformation tests
├── spark_docker/
│   ├── Dockerfile                      ← Spark Docker image
│   ├── docker-compose.yml              ← Test runner compose
│   └── spark_docker.md                 ← Docker instructions
├── docs/                               ← MkDocs documentation
├── mkdocs.yml
└── pyproject.toml                      ← All configuration
```

## Design Principles

### Mixed source patterns

| Module | Pattern | Description |
| --- | --- | --- |
| `data_processing.py` | Library functions | Importable pipeline functions |
| `reader/` | Library functions | Importable reader utilities |
| `transformation/` | Library functions | Importable transformations |
| `utility/` | Standalone scripts | Runnable Faker generators |

### Test mirrors source

```
src/data_processing.py        → tests/test_data_processing.py
src/reader/spark_reader.py    → tests/reader/test_spark_reader.py
src/transformation/           → tests/transformation/
```

### Single conftest.py

The shared SparkSession fixture lives in `tests/conftest.py`.
Individual test files use it via the `spark` parameter.
