# -- Project information -----------------------------------------------------

project = 'spark-general'
copyright = '2025, Mohammad Mahfooz Alam'
author = 'Mohammad Mahfooz Alam'

version = '0.1.0'
release = '0.1.0'

# -- General configuration ---------------------------------------------------

extensions = [
    'sphinx.ext.todo',
    "myst_parser",
    "sphinxcontrib.mermaid",
    "sphinx.ext.graphviz",
    "sphinxcontrib.plantuml",
]

myst_enable_extensions = [
  'colon_fence',
  'attrs_block',
]

templates_path = ['_templates']
exclude_patterns = []

language = 'en'

# -- Options for HTML output -------------------------------------------------

html_theme = 'sphinxawesome_theme'
html_static_path = ['_static']
html_permalinks = False

# -- Options for todo extension ----------------------------------------------

todo_include_todos = True
