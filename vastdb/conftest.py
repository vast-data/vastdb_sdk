import os
import sqlite3
from pathlib import Path
from typing import Iterable

import boto3
import pytest

import vastdb
import vastdb.errors
from vastdb.schema import Schema
from vastdb.session import Session

AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"


def get_aws_cred_from_system(cred_name):
    orion_dir = Path(os.path.expanduser(os.environ.get("ORION_DIR", "~/orion")))
    orion_file_path = orion_dir / 'data' / cred_name

    env_value = os.environ.get(cred_name)

    if env_value is not None:
        return env_value

    if not orion_file_path.exists():
        return None

    with open(orion_file_path, "r") as f:
        return f.read().strip()


def pytest_addoption(parser):
    parser.addoption("--tabular-bucket-name", help="Name of the S3 bucket with Tabular enabled", default="vastdb")
    parser.addoption("--tabular-access-key", help="Access key with Tabular permissions (AWS_ACCESS_KEY_ID)",
                     default=get_aws_cred_from_system(AWS_ACCESS_KEY_ID))
    parser.addoption("--tabular-secret-key", help="Secret key with Tabular permissions (AWS_SECRET_ACCESS_KEY)",
                     default=get_aws_cred_from_system(AWS_SECRET_ACCESS_KEY))
    parser.addoption("--tabular-endpoint-url", help="Tabular server endpoint", default=[], action="append")
    parser.addoption("--data-path", help="Data files location", default=None)
    parser.addoption("--crater-path", help="Save benchmark results in a dedicated location", default=None)
    parser.addoption("--schema-name", help="Name of schema for the test to operate on", default=None)
    parser.addoption("--table-name", help="Name of table for the test to operate on", default=None)
    parser.addoption("--num-workers", help="Number of concurrent workers", default=1)


@pytest.fixture(scope="session")
def session_kwargs(request: pytest.FixtureRequest, tabular_endpoint_urls):
    return dict(
        access=request.config.getoption("--tabular-access-key"),
        secret=request.config.getoption("--tabular-secret-key"),
        endpoint=tabular_endpoint_urls[0],
    )


@pytest.fixture(scope="session")
def session(session_kwargs):
    return vastdb.connect(**session_kwargs)


@pytest.fixture(scope="session")
def num_workers(request: pytest.FixtureRequest):
    return int(request.config.getoption("--num-workers"))


@pytest.fixture(scope="session")
def test_bucket_name(request: pytest.FixtureRequest):
    return request.config.getoption("--tabular-bucket-name")


@pytest.fixture(scope="session")
def tabular_endpoint_urls(request: pytest.FixtureRequest):
    return request.config.getoption("--tabular-endpoint-url") or ["http://localhost:9090"]


def iter_schemas(schema: Schema) -> Iterable[Schema]:
    """Recursively scan all schemas."""
    children = schema.schemas()
    for child in children:
        yield from iter_schemas(child)
    yield schema


@pytest.fixture(scope="function")
def clean_bucket_name(request: pytest.FixtureRequest, test_bucket_name: str, session: Session) -> str:
    with session.transaction() as tx:
        b = tx.bucket(test_bucket_name)
        for top_schema in b.schemas():
            for s in iter_schemas(top_schema):
                for t_name in s.tablenames():
                    try:
                        t = s.table(t_name)
                        if t is not None:
                            t.drop()
                    except vastdb.errors.NotSupportedSchema:
                        # Use internal API to drop the table in case unsupported schema prevents creating a table
                        # object.
                        tx._rpc.api.drop_table(b.name, s.name, t_name, txid=tx.txid)
                s.drop()
    return test_bucket_name


@pytest.fixture(scope="session")
def s3(request: pytest.FixtureRequest, tabular_endpoint_urls):
    return boto3.client(
        's3',
        aws_access_key_id=request.config.getoption("--tabular-access-key"),
        aws_secret_access_key=request.config.getoption("--tabular-secret-key"),
        endpoint_url=tabular_endpoint_urls[0])


@pytest.fixture(scope="function")
def parquets_path(request: pytest.FixtureRequest):
    return Path(request.config.getoption("--data-path"))


@pytest.fixture(scope="function")
def crater_path(request: pytest.FixtureRequest):
    return request.config.getoption("--crater-path")


@pytest.fixture(scope="function")
def schema_name(request: pytest.FixtureRequest):
    return request.config.getoption("--schema-name")


@pytest.fixture(scope="function")
def table_name(request: pytest.FixtureRequest):
    return request.config.getoption("--table-name")


@pytest.fixture(scope="function")
def perf_metrics_db(crater_path):
    return sqlite3.connect(f"{crater_path}/metrics.sqlite" if crater_path else ":memory:")
