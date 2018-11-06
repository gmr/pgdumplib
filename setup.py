from os import path
import setuptools

from pgdumplib import __version__


def read_requirements(name):
    requirements = []
    try:
        with open(path.join('requires', name)) as req_file:
            for line in req_file:
                if '#' in line:
                    line = line[:line.index('#')]
                line = line.strip()
                if line.startswith('-r'):
                    requirements.extend(read_requirements(line[2:].strip()))
                elif line and not line.startswith('-'):
                    requirements.append(line)
    except IOError:
        pass
    return requirements


setuptools.setup(
    name='pgdumplib',
    version=__version__,
    description='Library for reading PostgreSQL backups created with pg_dump',
    long_description=open('README.rst').read(),
    author='Gavin M. Roy',
    author_email='gavinmroy@gmail.com',
    url='https://github.com/gmr/pgdumplib',
    license='BSD',
    packages=['pgdumplib'],
    package_data={'': ['LICENSE', 'README.rst']},
    include_package_data=True,
    tests_require=read_requirements('testing.txt'),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules'],
    test_suite='nose.collector',
    zip_safe=True)
