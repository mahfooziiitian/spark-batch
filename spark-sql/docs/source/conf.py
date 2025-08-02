# -- Project information -----------------------------------------------------
project = 'Spark SQL'
copyright = '2025, Mohammad Mahfooz Alam'
author = 'Mohammad Mahfooz Alam'

version = '0.1.0'
release = '0.1.0'

# -- General configuration ---------------------------------------------------
extensions = [
    'sphinx.ext.todo',
    'myst_parser',
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'sphinxcontrib.mermaid',
]

templates_path = ['_templates']
exclude_patterns = []

language = 'en'

# -- Options for HTML output -------------------------------------------------
html_theme = 'sphinxawesome_theme'
html_static_path = ['_static']

# -- Options for todo extension ----------------------------------------------
todo_include_todos = True
