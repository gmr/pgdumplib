import datetime
import os
import sys

import pkg_resources

sys.path.insert(0, os.path.abspath('..'))
master_doc = 'index'
project = 'pgdumplib'
release = version = pkg_resources.get_distribution(project).version
copyright = '{}, Gavin M. Roy'.format(datetime.date.today().year)

extensions = [
    'sphinx.ext.autodoc',
    'sphinx_autodoc_typehints',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    'sphinx.ext.viewcode'
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']
intersphinx_mapping = {'python': ('https://docs.python.org/3', None)}
