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
html_theme = 'furo'
html_static_path = ['_static']
html_css_files = [
    'custom.css',
]
html_theme_options = {
    # "light_logo": "logo-light.png",  # optional
    # "dark_logo": "logo-dark.png",    # optional
    "sidebar_hide_name": False,
    "navigation_with_keys": True,
}
html_permalinks = False

# -- Options for todo extension ----------------------------------------------
todo_include_todos = True
