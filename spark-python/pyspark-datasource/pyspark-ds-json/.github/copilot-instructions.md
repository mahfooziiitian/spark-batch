# PySpark JSON Datasource — Copilot Instructions

## Project Overview

Comprehensive PySpark JSON datasource reference covering every Spark JSON option, function, schema approach, and error handling mode. The project contains ~55 standalone Python scripts organized by topic, each demonstrating a specific JSON feature or pattern.

## Tech Stack

- **Python** ≥ 3.11
- **PySpark** ~3.5.0
- **Pandas** (for JSON-to-DataFrame bridge)
- **Build**: setuptools (pyproject.toml with `setuptools.build_meta` backend)
- **Dependencies**: `requirements.txt` (`pyspark~=3.5.0`, `pandas`, `setuptools~=65.6.3`, `pytest`)
- **Testing**: pytest with `pythonpath = ["src"]` and `testpaths = ["tests"]`

## Source Structure

```
src/
├── jsons/
│   ├── data_source/                    # spark.read.json / .format("json").load()
│   ├── json_function/                  # Built-in JSON functions
│   │   ├── from_json/                  # from_json: string→struct, string→map, array→ArrayType
│   │   ├── to_json/                    # to_json: struct→JSON string
│   │   ├── json_tuple/                 # json_tuple: extract multiple keys
│   │   ├── json_object/               # json_object, json_object_keys
│   │   ├── json_array_length/         # json_array_length
│   │   └── schema_of_json/            # schema_of_json: infer schema from sample
│   ├── df/                             # DataFrame patterns
│   │   ├── json_array/                 # JSON array loading, array structures
│   │   └── json_pandas/               # Pandas bridge (pd.read_json → spark DF)
│   ├── properties/                     # Every JSON read/write option
│   │   ├── encoding/                   # UTF-8, UTF-16 (BE/LE BOM), UTF-32
│   │   ├── compression/               # gzip, bzip2, deflate, lz4, snappy, none
│   │   ├── line_separator/            # lineSep option
│   │   ├── comment/                   # allowComments
│   │   ├── quoting/                   # quote characters
│   │   ├── escaping_character/        # escape characters
│   │   ├── formatting/               # date/time format, timestamp NTZ, timezone
│   │   ├── fields/null_fields/        # dropFieldIfAllNull, ignoreNullFields
│   │   ├── primitives_as_string/      # primitivesAsString
│   │   ├── sampling_ratio/            # samplingRatio
│   │   ├── prefers_decimal/           # prefersDecimal
│   │   ├── leading_zero/             # allowNumericLeadingZeros
│   │   ├── numbers/                  # allowNonNumericNumbers
│   │   └── localization/             # locale
│   ├── corrupted_record/              # Error handling modes
│   │   ├── modes/permissive/          # PERMISSIVE (default) with _corrupt_record
│   │   ├── modes/drop_malformed/      # DROPMALFORMED
│   │   └── modes/fail_fast/           # FAILFAST
│   ├── rescued_data_column/           # Rescued data column pattern
│   ├── schema/                        # Schema approaches
│   │   ├── class_schema/             # StructType class-based
│   │   ├── data_class/               # Python dataclass → schema
│   │   ├── json_schema/              # JSON schema string
│   │   ├── keys/                     # Polymorphic/variable keys
│   │   └── schema_string/            # DDL string schema
│   └── json_path/                     # JSONPath expressions
├── utility/
│   └── create_json.py                 # JSON file generator
tests/
└── df/
    └── json_df_test.py
```

## Modular Instruction Files

| File | Scope | Purpose |
|------|-------|---------|
| `instructions/python.instructions.md` | `**/*.py` | Python style and conventions |
| `instructions/pyspark-json.instructions.md` | `src/**/*.py` | JSON datasource patterns and Spark API usage |
| `instructions/testing.instructions.md` | `tests/**/*.py` | pytest conventions and SparkSession fixtures |
| `instructions/project-config.instructions.md` | `pyproject.toml`, `requirements.txt` | Package configuration and build setup |

## Things to Avoid

- Do not use `findspark` — PySpark is installed via pip and available directly.
- Do not use `spark-submit` — scripts are run directly with `python`.
- Do not hardcode absolute file paths — use `os.path` or `pathlib.Path` relative to the script.
- Do not mix read options and write options — they are distinct sets in Spark's JSON API.
- Do not use deprecated `spark.read.load(path, format="json")` — prefer `spark.read.json(path)` or `.format("json").load(path)`.
- Do not create SparkSession without checking `SPARK_MASTER` env var for master URL.
- Do not skip `spark.stop()` — always stop the session at the end of each script.
