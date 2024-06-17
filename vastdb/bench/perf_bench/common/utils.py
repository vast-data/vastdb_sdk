import importlib.util
import logging
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from types import MappingProxyType, ModuleType
from typing import (
    Any,
    Dict,
    Generator,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

import exchange_calendars as xcal
import fsspec
import pandas as pd
from fsspec import AbstractFileSystem

# noinspection PyProtectedMember
from pandas._typing import IntervalClosedType
from pyarrow import dataset as ds

from vastdb.bench.perf_bench.common.constants import (
    DEFAULT_ACCESS_KEY,
    DEFAULT_END_T,
    DEFAULT_ROW_GROUP_SIZE,
    DEFAULT_S3_HOST,
    DEFAULT_SECRET_KEY,
    DEFAULT_START_T,
    DFAULT_PARQUET_COMPRESSION,
    VASTDB_ENDPOINT,
    ParquetCompression,
)
from vastdb.bench.perf_bench.common.log_utils import get_logger
from vastdb.bench.perf_bench.common.types import DateLikeT, PathLikeT
from vastdb.session import Session

_FS_MAP = MappingProxyType({"/mnt": "nfs", "/": "fs", "s3://": "s3", "": "vastdb"})


def config_ipython():
    try:
        # noinspection PyUnresolvedReferences
        _ = get_ipython
        pd.set_option("display.float_format", lambda x: "%.6f" % x)
        pd.set_option("display.max_rows", 50)
        pd.set_option("display.max_columns", 50)
        pd.set_option("display.width", 1000)
    except NameError:
        ...


# If in interactive mode, make it pretty
config_ipython()


def getenv_flag(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).lower() in ("1", "true", "yes")


def to_ts(d: Optional[DateLikeT], normalize: bool = True) -> Optional[pd.Timestamp]:
    if not d:
        return None
    ret_d: pd.Timestamp = d if isinstance(d, pd.Timestamp) else pd.Timestamp(d)
    return ret_d.normalize() if normalize else ret_d


def _default_s3_kwargs(
    aws_access_key_id: str = DEFAULT_ACCESS_KEY,
    aws_secret_access_key: str = DEFAULT_SECRET_KEY,
    host: str = DEFAULT_S3_HOST,
    config: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Dict[str, Any]:
    return {
        "endpoint_url": f"http://{host}",
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
        "use_ssl": False,
        "verify": False,
        "config": {
            "signature_version": "s3v4",
            "s3": {"addressing_style": "path"},
            **(config or {}),
        },
        "region_name": "us-east-1",
        **kwargs,
    }


def get_vastdb_session(
    access: str = DEFAULT_ACCESS_KEY,
    secret: str = DEFAULT_SECRET_KEY,
    vastdb_endpoint: str = VASTDB_ENDPOINT,
    ssl_verify: bool = True,
) -> Session:
    return Session(
        endpoint=vastdb_endpoint, access=access, secret=secret, ssl_verify=ssl_verify
    )


def get_filesystem(
    path: PathLikeT,
    botocore_client_kwargs: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Tuple[str, AbstractFileSystem]:
    path = str(path)
    if path.startswith("s3://"):
        botocore_client_kwargs = _default_s3_kwargs(
            **(botocore_client_kwargs or {}),
            config=kwargs.pop("config_kwargs", None),
        )
        fs = fsspec.filesystem(
            protocol="s3",
            client_kwargs=botocore_client_kwargs,
            **kwargs,
        )
        path = path[5:]  # remove the s3:// prefix
    else:
        fs = fsspec.filesystem(protocol="file", **kwargs)
    return path, fs


# noinspection PyShadowingBuiltins
def get_parquet_dataset(
    path: PathLikeT,
    filesystem: Optional[AbstractFileSystem] = None,
    fs_kwargs: Optional[Dict[str, Any]] = None,
    format: Optional[str] = "parquet",  # noqa: A002
    partitioning: Optional[str] = "hive",
    **kwargs,
) -> ds.Dataset:
    path, fs = filesystem or get_filesystem(path=path, **(fs_kwargs or {}))
    return ds.dataset(
        source=path,
        format=format,
        partitioning=partitioning,
        filesystem=fs,
        **kwargs,
    )


def get_parquet_dataset_root(
    base_dir: PathLikeT,
    row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
    compression: Union[str, ParquetCompression] = DFAULT_PARQUET_COMPRESSION,
) -> Path:
    return Path(str(base_dir)) / f"rg_{row_group_size!s}_c_{ParquetCompression[compression]}"


@contextmanager
def time_me(logger: Optional[logging.Logger] = None):
    logger = logger or get_logger(__name__)
    start_t = time.time_ns()
    yield  # Yield control back to the context block
    total_t_sec = (time.time_ns() - start_t) / 1e9
    logger.debug(f"Execution time: {total_t_sec:.2f} s")


S = TypeVar("S", bound=Sequence)


def chunk_sequence(input_seq: S, chunk_size: int) -> Generator[Sequence, None, None]:
    for i in range(0, len(input_seq), chunk_size):
        yield input_seq[i: i + chunk_size]


def get_dates_range(
    from_t: Optional[DateLikeT] = DEFAULT_START_T,
    to_t: Optional[DateLikeT] = DEFAULT_END_T,
    only_bdays: bool = True,
    inclusive: IntervalClosedType = "both",
) -> pd.DatetimeIndex:
    fun = pd.bdate_range if only_bdays else pd.date_range
    return fun(  # type: ignore[operator]
        start=to_ts(from_t),
        end=to_ts(to_t),
        inclusive=inclusive,
    )


def infer_fs_type(path: str) -> str:
    return next(k for m, k in _FS_MAP.items() if str(path).lower().startswith(m))


def get_session_minutes(
    date: DateLikeT,
    exchange: str = "XNYS",
) -> pd.DatetimeIndex:
    ecal = xcal.get_calendar(exchange)
    date = pd.Timestamp(date).normalize()
    sess = ecal.date_to_session(date=date, direction="next")
    sess_minutes = ecal.session_minutes(sess)
    return sess_minutes


def load_module_from_path(module_file_path: PathLikeT) -> ModuleType:
    file_path = Path(str(module_file_path)).resolve()
    module_name = file_path.stem

    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None:
        raise ImportError(f"Cannot find module {module_name} at {file_path}")

    module = importlib.util.module_from_spec(spec)
    loader = spec.loader
    if loader is None or not hasattr(loader, "exec_module"):
        raise ImportError(f"Cannot load module {module_name} from {file_path}")

    loader.exec_module(module)
    sys.modules[module_name] = module

    return module
