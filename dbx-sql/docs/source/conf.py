import sys
from pathlib import Path
from sphinxawesome_theme.postprocess import Icons

sys.path.insert(0, str(Path(__file__).parents[2]))
sys.path.insert(1, str(Path(__file__).parents[2] / "src"))


# -- Project information -----------------------------------------------------
project = 'dbx-sql'
copyright = '2025, Mohammad Mahfooz Alam'
author = 'Mohammad Mahfooz Alam'

# -- General configuration ---------------------------------------------------

master_doc = "index"

extensions = [
    "sphinxcontrib.mermaid",
    "sphinx_needs",
    "sphinxcontrib.test_reports",
    # "sphinxcontrib.plantuml",
    "autoapi.extension",
]

extensions.append("myst_parser")
extensions.append("autoapi.extension")
extensions.append("sphinx.ext.coverage")

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
html_theme = "sphinxawesome_theme"
source_suffix = {
    ".rst": "restructuredtext",
    ".txt": "markdown",
    ".md": "markdown",
}
html_static_path = ['_static']
html_permalinks_icon = Icons.permalinks_icon
