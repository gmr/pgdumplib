import datetime
from importlib import metadata

master_doc = 'index'
project = 'pgdumplib'
release = metadata.version('pgdumplib')
copyright = \
    f'{datetime.datetime.now(tz=datetime.UTC).date().year}, Gavin M. Roy'

extensions = [
    'sphinx.ext.autodoc', 'sphinx_autodoc_typehints', 'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx', 'sphinx.ext.viewcode'
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']
intersphinx_mapping = {'python': ('https://docs.python.org/3', None)}
