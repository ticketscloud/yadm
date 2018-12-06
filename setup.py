from setuptools import setup

# python setup.py sdist --formats=bztar

version = '2.0.2'
description = 'Yet Another Document Mapper (ODM) for MongoDB'
long_description = open('README.rst', 'rb').read().decode('utf8')


setup(
    name='yadm',
    version=version,
    description=description,
    long_description=long_description,
    author='Zelenyak "ZZZ" Alexander',
    author_email='zzz.sochi@gmail.com',
    url='https://github.com/zzzsochi/yadm',
    license='BSD',
    platforms='any',
    install_requires=[
        'pymongo>=3.7.0',
        'motor>=2.0.0',
        'zope.dottedname',
        'python-dateutil',
        'pytz',
        'Faker',
    ],

    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Topic :: Database',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
    ],

    packages=['yadm', 'yadm.aio', 'yadm.fields', 'yadm.fields.money'],
)
