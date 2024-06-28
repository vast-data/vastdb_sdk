"""Configuration file for the Sphinx documentation builder.

For the full list of built-in configuration values, see the documentation:
https://www.sphinx-doc.org/en/master/usage/configuration.html

-- Project information -----------------------------------------------------
https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
"""
import os
import sys

project = 'VAST DB Python SDK'
copyright = '2024, VAST Data'
author = 'VAST Data'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

current_dir = os.path.dirname(__file__)
vastdb_path = os.path.abspath(os.path.join(current_dir, '..', '..', 'vastdb'))
sys.path.insert(0, vastdb_path)

html_domain_indices = True

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.intersphinx',
    'sphinx_rtd_theme',
    'myst_parser'
]

templates_path = ['_templates']
exclude_patterns = [
    '../../vastdb/vast_flatbuf',
]

autodoc_mock_imports = [
    "flatbuffers",
    "ibis",
    "pyarrow",
    "xmltodict",
    "aws_requests_auth",
    "backoff"
    ]

source_suffix = {
    '.rst': 'restructuredtext',
    '.txt': 'markdown',
    '.md': 'markdown',
}

intersphinx_mapping = {
    "pyarrow": ("https://arrow.apache.org/docs/", None),
}

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# html_theme = 'alabaster'
# html_theme = "pydata_sphinx_theme"
html_theme = "sphinx_rtd_theme"

html_static_path = ['_static']

# def setup(app):
#     app.add_css_file('custom.css')
