import sys
from pathlib import Path

sys.path.insert(1, str(Path(__file__).parents[2] / "src"))


# -- Project information -----------------------------------------------------
project = "pyspark-rdd"
copyright = "2025, Mohammad Mahfooz Alam."
author = "Mohammad Mahfooz Alam"

# -- General configuration ---------------------------------------------------

master_doc = "index"

extensions = [
    "sphinxcontrib.mermaid",
    "sphinx_needs",
    "sphinx_design",
    "myst_parser",
    # "autoapi.extension",
    "sphinx.ext.coverage",
]

myst_enable_extensions = [
    "amsmath",
    "colon_fence",
    "deflist",
    "dollarmath",
    "linkify",
    "substitution",
]

templates_path = ["_templates"]
exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
]

autoapi_dirs = [
    "../../src",
]

# -- Options for HTML output -------------------------------------------------
html_theme = "furo"
source_suffix = {
    ".rst": "restructuredtext",
    ".txt": "markdown",
    ".md": "markdown",
}
