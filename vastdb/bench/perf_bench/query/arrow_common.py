# ruff: noqa: F841

import datetime as dt
from functools import reduce
from typing import Optional, Sequence

import pyarrow as pa
from pyarrow import dataset as ds

from vastdb.bench.perf_bench.common import utils as bu
from vastdb.bench.perf_bench.dataset import secmaster as sm
from vastdb.bench.perf_bench.dataset.schemas import BF


def build_arrow_filter(
    universe: Sequence[str],
    from_t: Optional[dt.datetime] = None,
    to_t: Optional[dt.datetime] = None,
    use_sid: bool = True,
) -> ds.Expression:
    # Build the filters
    y_m_filter = from_t_filter = to_t_filter = None
    from_t, to_t = bu.to_ts(from_t, normalize=False), bu.to_ts(to_t, normalize=False)
    if from_t or to_t:
        all_dates = bu.get_dates_range(from_t, to_t, inclusive="left")
        years = sorted({d.year for d in all_dates})

        y_m_filter = ds.field("year").isin(years) & ds.field("date").isin(
            tuple(d.strftime("%Y%m%d") for d in all_dates)
        )

        if from_t and from_t.time() != dt.time.min:
            from_t_filter = ds.field("ts") >= pa.scalar(from_t)
        if to_t and to_t.time() != dt.time.min:
            to_t_filter = ds.field("ts") < pa.scalar(to_t)

    if use_sid:
        universe = [sm.to_sid(s) for s in universe]  # type: ignore[misc]
        fld = BF.sid.value
    else:
        fld = BF.ticker.value
    universe = sorted(universe)
    if len(universe) <= 16:
        tickers_filter = reduce(
            lambda x, y: x | y, [ds.field(fld) == s for s in universe]
        )
    else:
        tickers_filter = ds.field(fld).isin(universe)

    ds_filter = reduce(
        lambda x, y: x & y,
        [
            flt
            for flt in (y_m_filter, tickers_filter, from_t_filter, to_t_filter)
            if flt is not None
        ],
    )

    return ds_filter
