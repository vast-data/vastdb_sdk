import pytest


def pytest_addoption(parser):
    parser.addoption("--test-bucket-name", help="Name of the S3 bucket to be created for Tabular test")


@pytest.fixture
def test_bucket_name(request):
    return request.config.getoption("--test-bucket-name")
