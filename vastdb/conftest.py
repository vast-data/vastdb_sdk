import vastdb

import pytest
import boto3
import os
from pathlib import Path


def pytest_addoption(parser):
    parser.addoption("--tabular-bucket-name", help="Name of the S3 bucket with Tabular enabled", default="vastdb")
    parser.addoption("--tabular-access-key", help="Access key with Tabular permissions (AWS_ACCESS_KEY_ID)", default=os.environ.get("AWS_ACCESS_KEY_ID", None))
    parser.addoption("--tabular-secret-key", help="Secret key with Tabular permissions (AWS_SECRET_ACCESS_KEY)", default=os.environ.get("AWS_SECRET_ACCESS_KEY", None))
    parser.addoption("--tabular-endpoint-url", help="Tabular server endpoint", default="http://localhost:9090")
    parser.addoption("--data-path", help="Data files location", default=None)
    parser.addoption("--crater-path", help="Save benchmark results in a dedicated location", default=None)


@pytest.fixture(scope="session")
def session(request):
    return vastdb.connect(
        access=request.config.getoption("--tabular-access-key"),
        secret=request.config.getoption("--tabular-secret-key"),
        endpoint=request.config.getoption("--tabular-endpoint-url"),
    )


@pytest.fixture(scope="session")
def test_bucket_name(request):
    return request.config.getoption("--tabular-bucket-name")


@pytest.fixture(scope="function")
def clean_bucket_name(request, test_bucket_name, session):
    with session.transaction() as tx:
        b = tx.bucket(test_bucket_name)
        for s in b.schemas():
            for t in s.tables():
                t.drop()
            s.drop()
    return test_bucket_name


@pytest.fixture(scope="session")
def s3(request):
    return boto3.client(
        's3',
        aws_access_key_id=request.config.getoption("--tabular-access-key"),
        aws_secret_access_key=request.config.getoption("--tabular-secret-key"),
        endpoint_url=request.config.getoption("--tabular-endpoint-url"))


@pytest.fixture(scope="function")
def parquets_path(request):
    return Path(request.config.getoption("--data-path"))


@pytest.fixture(scope="function")
def crater_path(request):
    return request.config.getoption("--crater-path")
