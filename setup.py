import setuptools

from pgdumplib import __version__


setuptools.setup(
    name='pgdumplib',
    version=__version__,
    description='Library for PostgreSQL backups created with pg_dump',
    author='Gavin M. Roy',
    author_email='gavinmroy@gmail.com',
    url='https://github.com/gmr/pgdumplib',
    license='BSD',
    packages=['pgdumplib'],
    package_data={'': ['LICENSE', 'README.rst']},
    include_package_data=True,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules'],
    test_suite='nose.collector',
    zip_safe=True)
