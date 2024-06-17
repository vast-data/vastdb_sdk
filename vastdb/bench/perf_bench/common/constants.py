import logging
import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from urllib3.util import parse_url

from vastdb.bench.perf_bench.common.types import StrEnum

_MY_DIR = Path(__file__).parent

# VastDB details
VASTDB_ENDPOINT = os.getenv("VASTDB_ENDPOINT", "")
VASTDB_BUCKET_NAME = os.getenv("VASTDB_BUCKET_NAME", "")
VASTDB_TEST_SCHEMA_NAME = os.getenv("VASTDB_TEST_SCHEMA_NAME", "")
VASTDB_TEST_TABLE_NAME = os.getenv("VASTDB_TEST_TABLE_NAME", "")

# Regular S3 details
S3_BUCKET_NAME = "my-s3-bucket"
DEFAULT_S3_SSL_PORT = int(os.getenv("DEFAULT_S3_SSL_PORT", 443))
if DEFAULT_S3_ENDPOINT_URL := os.getenv("AWS_S3_ENDPOINT_URL", ""):
    _parsed = parse_url(DEFAULT_S3_ENDPOINT_URL)
    DEFAULT_S3_HOST = str(_parsed.host)
    DEFAULT_S3_PORT = int(_parsed.port or 80)
else:
    DEFAULT_S3_HOST = os.getenv("DEFAULT_S3_HOST", "1.1.1.1")
    DEFAULT_S3_PORT = int(os.getenv("DEFAULT_S3_PORT", 80))
    DEFAULT_S3_ENDPOINT_URL = f"http://{DEFAULT_S3_HOST}:{DEFAULT_S3_PORT}"

# Paths
DEFAULT_RESULTS_DIR = Path(__file__).parent.parent / "benchmark_results"
NFS_DS_PATH = f"/mnt/data/{VASTDB_BUCKET_NAME}/data"
LOCAL_FS_DS_PATH = _MY_DIR.parent / "dataset" / "test_dataset"
S3_DS_PATH = f"s3://{VASTDB_BUCKET_NAME}/data"

# Access keys
DEFAULT_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "some_access_key")
DEFAULT_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "some_secret_key")

# Dataset start/end dates
DEFAULT_START_T = pd.Timestamp("20180101")
DEFAULT_END_T = pd.Timestamp("20200201")

# Arrow related constants
DEFAULT_ROW_GROUP_SIZE = 1024 * 1024
DEFAULT_ARROW_KWARGS = {
    "batch_size": (DEFAULT_ARROW_BATCH_SIZE := 786432),
    "batch_readahead": (DEFAULT_ARROW_BATCH_READAHEAD := 12),
    "fragment_readahead": (DEFAULT_ARROW_BATCH_FRAGMENT_READAHEAD := 4),
}


class ParquetCompression(StrEnum):
    NONE = "NONE"
    SNAPPY = "SNAPPY"
    GZIP = "GZIP"
    # LZO = "LZO"
    BROTLI = "BROTLI"
    LZ4 = "LZ4"
    ZSTD = "ZSTD"


DFAULT_PARQUET_COMPRESSION = ParquetCompression.LZ4


class LogLevel(StrEnum):
    CRITICAL = "CRITICAL"
    FATAL = "FATAL"
    ERROR = "ERROR"
    WARN = "WARNING"
    WARNING = "WARNING"
    INFO = "INFO"
    DEBUG = "DEBUG"
    NOTSET = "NOTSET"

    def to_int(self) -> int:
        # noinspection PyUnresolvedReferences,PyProtectedMember
        return logging._nameToLevel[self]  # noqa: SLF001


@dataclass(frozen=True)
class VastConnDetails:
    """VAST Config."""

    access: str = DEFAULT_ACCESS_KEY
    secret: str = DEFAULT_SECRET_KEY
    vastdb_bucket: str = VASTDB_BUCKET_NAME
    vastdb_endpoint: str = VASTDB_ENDPOINT
    vastdb_ssl_verify: bool = True
    vastdb_schema: str = VASTDB_TEST_SCHEMA_NAME
    vastdb_table: str = VASTDB_TEST_TABLE_NAME
    s3_host: str = DEFAULT_S3_HOST
    s3_bucket: str = VASTDB_BUCKET_NAME
    s3_port: int = DEFAULT_S3_PORT
    s3_ssl_port: int = DEFAULT_S3_SSL_PORT
