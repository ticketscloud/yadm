from setuptools import setup

# python setup.py sdist --formats=bztar

description = 'Yet Another Document Mapper (ODM) for MongoDB'
long_description = open('README.rst', 'rb').read().decode('utf8')


setup(
    name='yadm',
    version='0.3',
    description=description,
    long_description=long_description,
    author='Zelenyak Aleksander aka ZZZ',
    author_email='zzz.sochi@gmail.com',
    url='https://github.com/zzzsochi/yadm',
    license='GPL',
    platforms='any',
    install_requires=['structires, pymongo']

    classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Intended Audience :: Developers',
            'Operating System :: OS Independent',
            'Programming Language :: Python :: 3',
          ],

    packages=['yadm'],
)
