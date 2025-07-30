# -- Project information -----------------------------------------------------

project = 'spark-sql-function'
copyright = '2025, Mohammad Mahfooz Alam'
author = 'Mohammad Mahfooz Alam'
release = '0.1.0'

# -- General configuration ---------------------------------------------------
extensions = [
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'sphinx.ext.todo',
    'sphinx.ext.graphviz',
    'sphinxcontrib.mermaid',
    'myst_parser',
]

templates_path = ['_templates']
exclude_patterns = []

language = 'en'

# -- Options for HTML output -------------------------------------------------

html_theme = 'sphinxawesome_theme'
html_static_path = ['_static']
html_permalinks_icon = ''
# -- Options for todo extension ----------------------------------------------
todo_include_todos = True
