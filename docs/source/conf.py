import os
import sys
sys.path.insert(0, os.path.abspath('../../')) 
# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'TS-tools-replacement'
copyright = '2025, Hugo Pessolano'
author = 'Hugo Pessolano'
release = '0.1.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx.ext.intersphinx',
    # 'sphinx.ext.githubpages', # Si la habilitaste
    'sphinx_autodoc_typehints', # <-- AÑADE ESTA
]

templates_path = ['_templates']
exclude_patterns = []

language = 'es'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']


napoleon_google_docstring = True
napoleon_numpy_docstring = False 
napoleon_include_init_with_doc = True 
napoleon_include_private_with_doc = False 
napoleon_use_admonition_for_examples = False
napoleon_use_ivar = True 
napoleon_use_param = True
napoleon_use_rtype = True
intersphinx_mapping = {'python': ('https://docs.python.org/3', None)}
typehints_fully_qualified = False # Mostrar 'str' en lugar de 'typing.str'
always_document_param_types = True # Mostrar tipos incluso si están en docstring
typehints_document_rtype = True # Documentar tipo de retorno
