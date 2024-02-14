from setuptools import setup, find_packages

long_description = """
`VastdbApi` is a Python based API designed for interacting with *VastDB* & *Vast Catalog*, enabling operations such as schema and table management, data querying, and transaction handling.
Key libraries used in this API include requests for HTTP requests, pyarrow for handling Apache Arrow data formats, and flatbuffers for efficient serialization of data structures.


```
pip install vastdb
```

## Creating the initial session with VastdbApi:

```python
from vastdb import api
import pyarrow as pa
import vast_flatbuf
from vastdb.api import VastdbApi

def create_vastdb_session(access_key, secret_key):
    return VastdbApi(host='VAST_VIP_POOL_DNS_NAME', access_key=access_key, secret_key=secret_key)


access_key='D8UDFDF...'
secret_key='B7bqMegmj+TDN..'
vastdb_session = create_vastdb_session(access_key, secret_key)

```
#### For the complete Guide for the SDK please go to VastData github: https://github.com/vast-data/vastdb_sdk

"""

setup(
    name='vastdb',
    description='VAST Data SDK',
    version='0.0.4',
    url='https://github.com/vast-data/vastdb_sdk',
    author='VAST DATA',
    author_email='hello@vastdata.com',
    license='Copyright (C) VAST Data Ltd.',
    packages=find_packages(),
    install_requires=[
        'flatbuffers',
        'pyarrow',
        'requests',
        'aws-requests-auth',
        'xmltodict',
        'protobuf==3.19.6'
    ],
    long_description=long_description,
    long_description_content_type='text/markdown',
)
