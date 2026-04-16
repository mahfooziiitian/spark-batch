# Installation

## Dependencies

=== "poetry (recommended)"

    ```bash
    cd spark-python/pyspark-pyarrow
    poetry install
    ```

=== "pip"

    ```bash
    pip install "pyspark[sql]>=3.5.1" "pyarrow>=14.0.0" "pandas>=2.0.0" "numpy>=1.24.0"
    ```

=== "uv"

    ```bash
    uv add "pyspark[sql]" pyarrow pandas numpy
    ```

## Verify Installation

```bash
python -c "
import pyspark, pyarrow, pandas
print(f'PySpark {pyspark.__version__}')
print(f'PyArrow {pyarrow.__version__}')
print(f'Pandas  {pandas.__version__}')
"
```

!!! note "pyproject.toml"

    The project uses Poetry for dependency management. All dependencies are declared in
    `pyproject.toml`:

    ```toml
    [tool.poetry.dependencies]
    python = "^3.11"
    pyspark = {extras = ["sql"], version = "^3.5.1"}
    pyarrow = ">=14.0.0"
    pandas = ">=2.0.0"
    numpy = ">=1.24.0"
    ```

## Dev Dependencies

For running tests:

=== "poetry"

    ```bash
    poetry install  # includes dev deps by default
    ```

=== "pip"

    ```bash
    pip install pytest pytest-mock
    ```
