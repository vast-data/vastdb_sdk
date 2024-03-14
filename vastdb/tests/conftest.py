import pytest

from vastdb import v2

def pytest_addoption(parser):
    parser.addoption("--tabular-bucket-name", help="Name of the S3 bucket with Tabular enabled")
    parser.addoption("--tabular-access-key", help="Access key with Tabular permissions")
    parser.addoption("--tabular-secret-key", help="Secret key with Tabular permissions")
    parser.addoption("--tabular-endpoint-url", help="Tabular server endpoint")


@pytest.fixture(scope="module")
def rpc(request):
    return v2.connect(
        access=request.config.getoption("--tabular-access-key"),
        secret=request.config.getoption("--tabular-secret-key"),
        endpoint=request.config.getoption("--tabular-endpoint-url"),
    )

@pytest.fixture(scope="module")
def test_bucket_name(request):
    return request.config.getoption("--tabular-bucket-name")

@pytest.fixture(scope="module")
def clean_bucket_name(request, test_bucket_name, rpc):
    with rpc.transaction() as tx:
        b = tx.bucket(test_bucket_name)
        for s in b.schemas():
            s.drop()
    return test_bucket_name
