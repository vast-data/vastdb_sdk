# ruff: noqa: F841

import datetime as dt
import logging
from typing import Any, Dict, Optional, Sequence

import pyarrow as pa

import vastdb.bench.perf_bench.common.log_utils
from vastdb.bench.perf_bench.common import utils as bu
from vastdb.bench.perf_bench.common.constants import (
    DEFAULT_ARROW_KWARGS,
    VastConnDetails,
)
from vastdb.bench.perf_bench.query.arrow_common import build_arrow_filter

LOG = vastdb.bench.perf_bench.common.log_utils.get_logger(__name__)


# noinspection PyUnusedLocal
def query_pyarrow(
    universe: Sequence[str],
    columns: Optional[Sequence[str]] = None,
    from_t: Optional[dt.datetime] = None,
    to_t: Optional[dt.datetime] = None,
    path: Optional[str] = None,
    use_sid: bool = True,
    logger: Optional[logging.Logger] = None,
    backend_kwargs: Optional[Dict[str, Any]] = None,
) -> pa.Table:
    # ------------------------------------------------------------
    #    Query the PyArrow's parquet dataset
    # ------------------------------------------------------------
    backend_kwargs = backend_kwargs or {}
    conn_details: Optional[VastConnDetails] = backend_kwargs.pop("conn_details", None)
    conn_details = conn_details or VastConnDetails()
    fs_kwargs = {
        "botocore_client_kwargs": {
            "aws_access_key_id": conn_details.access,
            "aws_secret_access_key": conn_details.secret,
            "host": conn_details.s3_host,
        }
    }

    # Note that one can optimize the below by passing the filesystem object and/or partitioning
    #  scheme to avoid the slow penalty of discovering the partitioning upon init.
    dset = bu.get_parquet_dataset(path=path, filesystem=None, fs_kwargs=fs_kwargs)  # type: ignore[arg-type]

    ds_filter = build_arrow_filter(
        universe=universe,
        from_t=from_t,
        to_t=to_t,
        use_sid=use_sid,
    )

    # Cleanup the kwargs to be passed to the arrow scanner
    kwargs = backend_kwargs or DEFAULT_ARROW_KWARGS
    kwargs.pop("arrow_kwargs", None)
    if flt := kwargs.pop("filter", None):
        ds_filter = ds_filter & flt

    # Read the data
    # noinspection PyArgumentList
    table = dset.to_table(
        columns=columns,
        filter=ds_filter,
        **{**DEFAULT_ARROW_KWARGS, **(kwargs or {})},
    )

    return table
