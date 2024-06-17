# ruff: noqa: F841, PLW0603

import datetime as dt
import logging
from typing import Any, Dict, Optional, Sequence

import pyarrow as pa

import vastdb.bench.perf_bench.common.log_utils
from vastdb.bench.perf_bench.common import utils as bu
from vastdb.bench.perf_bench.common.constants import VastConnDetails
from vastdb.bench.perf_bench.dataset import secmaster as sm
from vastdb.bench.perf_bench.dataset.schemas import BF
from vastdb.table import QueryConfig

LOG = vastdb.bench.perf_bench.common.log_utils.get_logger(__name__)


# noinspection PyUnusedLocal,PyArgumentList
def query_vastdb(
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
    #    Query via VastDB
    # ------------------------------------------------------------
    kwargs = backend_kwargs or {}
    conn_details: Optional[VastConnDetails] = kwargs.pop("conn_details", None)
    conn_details = conn_details or VastConnDetails()
    ss = bu.get_vastdb_session(
        access=conn_details.access,
        secret=conn_details.secret,
        vastdb_endpoint=conn_details.vastdb_endpoint,
        ssl_verify=conn_details.vastdb_ssl_verify,
    )
    with ss.transaction() as tx:
        b = tx.bucket(conn_details.vastdb_bucket)
        s = b.schema(conn_details.vastdb_schema)
        t = s.table(conn_details.vastdb_table)

        if use_sid:
            fld = BF.sid.value
            sid_uni = sorted(sm.to_sid(s) for s in universe)
            filters = t[fld].isin(sid_uni)
        else:
            fld = BF.ticker.value
            filters = t[fld].isin(sorted(universe))

        if from_t and to_t:
            # noinspection PyTypedDict
            filters = filters & (t["ts"] >= from_t) & (t["ts"] < to_t)
        elif from_t:
            filters = filters & (t["ts"] >= from_t)
        elif to_t:
            filters.append(t["ts"] < to_t)

        # Cleanup the kwargs to be passed to the arrow scanner
        kwargs.pop("arrow_kwargs", None)
        if kwargs.pop("filter", None):
            raise ValueError("Can't use filter with VastDB query")

        # Perform the query
        # noinspection PyTypeChecker
        config = QueryConfig(**kwargs)
        endpoint = conn_details.vastdb_endpoint
        config.num_splits = 32
        config.num_sub_splits = 8
        config.data_endpoints = [endpoint] * config.num_splits
        table = pa.Table.from_batches(
            batches=t.select(predicate=filters, columns=columns, config=config)
        )
    return table
