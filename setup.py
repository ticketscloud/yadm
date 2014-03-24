from setuptools import setup

from yadm import __version__

# python setup.py sdist --formats=bztar

description = 'Yet Another Document Mapper (ODM) for MongoDB'
long_description = open('README.rst', 'rb').read().decode('utf8')


setup(
    name='yadm',
    version=__version__,
    description=description,
    long_description=long_description,
    author='Zelenyak Aleksander aka ZZZ',
    author_email='zzz.sochi@gmail.com',
    url='https://github.com/zzzsochi/yadm',
    license='BSD',
    platforms='any',
    install_requires=['structures, pymongo']

    classifiers=[
            'Development Status :: 3 - Alpha',
            'Topic :: Database',
            'Intended Audience :: Developers',
            'Operating System :: OS Independent',
            'Programming Language :: Python :: 3',
          ],

    packages=['yadm'],
)
