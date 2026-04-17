# Notebooks

Interactive Jupyter notebooks that mirror the Python scripts — useful for
step-by-step exploration in JupyterLab or VS Code.

## Available Notebooks

| Notebook | Description |
|----------|-------------|
| `find_spark_lib.ipynb` | Locate Spark with `findspark`, create a session, list databases |
| `read_pyspark_config.ipynb` | Load config from `.properties`, `.cfg`, and `.conf` files |
| `pyspark_config_options.ipynb` | List all Spark options, get/set at runtime, validate with `SparkConf` |

## Run

=== "JupyterLab"
    ```bash
    uv run --group docs pip install jupyterlab
    uv run jupyter lab notebooks/
    ```

=== "VS Code"
    Open any `.ipynb` file in VS Code with the
    [Jupyter extension](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter)
    installed. Select the `.venv` kernel.

!!! tip
    The notebooks reference config files via relative paths (`../cfg/`).
    Launch Jupyter from the project root or `notebooks/` directory.
