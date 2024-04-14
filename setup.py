import os

from setuptools import find_packages, setup

long_description = """
`vastdb` is a Python-based SDK designed for interacting
with [VAST Database](https://vastdata.com/database)
and [VAST Catalog](https://vastdata.com/blog/vast-catalog-treat-your-file-system-like-a-database),
enabling schema and table management, efficient ingest, query and modification of columnar data.

For more details, see [our whitepaper](https://vastdata.com/whitepaper/#TheVASTDataBase).
"""

def _get_version_suffix():
    import subprocess

    commit = subprocess.check_output(["git", "rev-parse", "HEAD"])
    print(f"Git commit: {commit}")
    return f".dev1+vast.{commit.decode()[:16]}"

suffix = ''
if os.environ.get('VASTDB_APPEND_VERSION_SUFFIX'):
    suffix = _get_version_suffix()

setup(
    name='vastdb',
    python_requires='>=3.9.0',
    description='VAST Data SDK',
    version='0.1.0' + suffix,
    url='https://github.com/vast-data/vastdb_sdk',
    author='VAST DATA',
    author_email='hello@vastdata.com',
    license='Copyright (C) VAST Data Ltd.',
    packages=find_packages(),
    install_requires=open('requirements.txt').read().strip().split('\n'),
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Topic :: Database',
        'Topic :: Database :: Front-Ends',
    ],
)
