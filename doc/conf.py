import sys
sys.path.insert(0, '.')

project = u'yadm'
copyright = u'2014, Alexander "ZZZ" Zelenyak'
version = '1.4.5'
release = version

extensions = ['sphinx.ext.autodoc']
templates_path = ['_templates']
source_suffix = '.rst'
source_encoding = 'utf8'
master_doc = 'index'
exclude_trees = ['_build']

# pygments_style = 'sphinx'
# html_theme = 'sphinxdoc'
# html_static_path = ['_static']
