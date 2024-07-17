import os
import sqlite3
from pathlib import Path

import boto3
import pytest

import vastdb


def pytest_addoption(parser):
    parser.addoption("--tabular-bucket-name", help="Name of the S3 bucket with Tabular enabled", default="vastdb")
    parser.addoption("--tabular-access-key", help="Access key with Tabular permissions (AWS_ACCESS_KEY_ID)",
                     default=os.environ.get("AWS_ACCESS_KEY_ID", None))
    parser.addoption("--tabular-secret-key", help="Secret key with Tabular permissions (AWS_SECRET_ACCESS_KEY)",
                     default=os.environ.get("AWS_SECRET_ACCESS_KEY", None))
    parser.addoption("--tabular-endpoint-url", help="Tabular server endpoint", default=[], action="append")
    parser.addoption("--data-path", help="Data files location", default=None)
    parser.addoption("--crater-path", help="Save benchmark results in a dedicated location", default=None)
    parser.addoption("--schema-name", help="Name of schema for the test to operate on", default=None)
    parser.addoption("--table-name", help="Name of table for the test to operate on", default=None)
    parser.addoption("--num-workers", help="Number of concurrent workers", default=1)


@pytest.fixture(scope="session")
def session_kwargs(request, tabular_endpoint_urls):
    return dict(
        access=request.config.getoption("--tabular-access-key"),
        secret=request.config.getoption("--tabular-secret-key"),
        endpoint=tabular_endpoint_urls[0],
    )


@pytest.fixture(scope="session")
def session(session_kwargs):
    return vastdb.connect(**session_kwargs)


@pytest.fixture(scope="session")
def num_workers(request):
    return int(request.config.getoption("--num-workers"))


@pytest.fixture(scope="session")
def test_bucket_name(request):
    return request.config.getoption("--tabular-bucket-name")


@pytest.fixture(scope="session")
def tabular_endpoint_urls(request):
    return request.config.getoption("--tabular-endpoint-url") or ["http://localhost:9090"]


def iter_schemas(s):
    """Recusively scan all schemas."""
    children = s.schemas()
    for c in children:
        yield from iter_schemas(c)
    yield s


@pytest.fixture(scope="function")
def clean_bucket_name(request, test_bucket_name, session):
    with session.transaction() as tx:
        b = tx.bucket(test_bucket_name)
        for top_schema in b.schemas():
            for s in iter_schemas(top_schema):
                for t in s.tables():
                    t.drop()
                s.drop()
    return test_bucket_name


@pytest.fixture(scope="session")
def s3(request, tabular_endpoint_urls):
    return boto3.client(
        's3',
        aws_access_key_id=request.config.getoption("--tabular-access-key"),
        aws_secret_access_key=request.config.getoption("--tabular-secret-key"),
        endpoint_url=tabular_endpoint_urls[0])


@pytest.fixture(scope="function")
def parquets_path(request):
    return Path(request.config.getoption("--data-path"))


@pytest.fixture(scope="function")
def crater_path(request):
    return request.config.getoption("--crater-path")


@pytest.fixture(scope="function")
def schema_name(request):
    return request.config.getoption("--schema-name")


@pytest.fixture(scope="function")
def table_name(request):
    return request.config.getoption("--table-name")


@pytest.fixture(scope="function")
def perf_metrics_db(crater_path):
    return sqlite3.connect(f"{crater_path}/metrics.sqlite" if crater_path else ":memory:")
