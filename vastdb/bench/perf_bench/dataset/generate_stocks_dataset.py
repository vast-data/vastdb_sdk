import concurrent.futures
import datetime as dt
import os
import time
from pathlib import Path
from typing import Optional, Sequence, Union

import numpy as np
import pandas as pd

from vastdb.bench.perf_bench.common.constants import (
    DEFAULT_END_T,
    DEFAULT_ROW_GROUP_SIZE,
    DEFAULT_START_T,
    DFAULT_PARQUET_COMPRESSION,
    LOCAL_FS_DS_PATH,
    ParquetCompression,
)
from vastdb.bench.perf_bench.common.log_utils import get_logger
from vastdb.bench.perf_bench.common.types import DateLikeT, PathLikeT
from vastdb.bench.perf_bench.common.utils import (
    get_parquet_dataset_root,
    get_session_minutes,
)
from vastdb.bench.perf_bench.dataset import secmaster as sm
from vastdb.bench.perf_bench.dataset.schemas import (
    BF,
    StockBarsPandasSchema,
)

LOG = get_logger(__name__)

DEFAULT_NUM_WORKERS = max(1, int((os.cpu_count() or 1) * 0.4))
DEFAULT_DS_ROOT_PATH = LOCAL_FS_DS_PATH

ref_bars: Optional[pd.DataFrame] = None


def generate_synthetic_stock_1m_bars_day(
    date: Optional[DateLikeT] = None,
    exchange: str = "XNYS",
) -> pd.DataFrame:
    sess_minutes = get_session_minutes(date=date or DEFAULT_START_T, exchange=exchange)
    uni_tickers = sm.large_uni
    sids = [sm.to_sid(t) for t in sm.large_uni]
    ref_px = pd.Series(
        [sm.get_indicative_px(sid) for sid in sids],
        dtype=BF.trade_close.pd_type,
    )
    uni_sz = len(uni_tickers)

    def _build_minute(m: pd.Timestamp) -> pd.DataFrame:
        return pd.DataFrame(
            {
                BF.sid.name: sids,
                BF.ts.name: [m.astimezone("UTC").tz_localize(None)] * uni_sz,  # type: ignore[arg-type]
                BF.ticker.name: uni_tickers,
                # Ask
                BF.ask_open.name: (
                    ao := ref_px * np.random.uniform(0.999, 0.001, uni_sz)
                ),
                BF.ask_high.name: ao * np.random.uniform(1.0, 1.001, uni_sz),
                BF.ask_low.name: ao * np.random.uniform(0.999, 1.0, uni_sz),
                BF.ask_close.name: ao * np.random.uniform(0.999, 1.001, uni_sz),
                BF.ask_qty.name: np.random.randint(1, 10000, uni_sz),
                # Bid
                BF.bid_open.name: (
                    bo := ref_px * np.random.uniform(0.999, 0.001, uni_sz)
                ),
                BF.bid_high.name: bo * np.random.uniform(1.0, 1.001, uni_sz),
                BF.bid_low.name: bo * np.random.uniform(0.999, 1.0, uni_sz),
                BF.bid_close.name: bo * np.random.uniform(0.999, 1.001, uni_sz),
                BF.bid_qty.name: np.random.randint(1, 10000, uni_sz),
                # Trades
                BF.trade_open.name: (
                    to := ref_px * np.random.uniform(0.999, 0.001, uni_sz)
                ),
                BF.trade_high.name: to * np.random.uniform(1.0, 1.001, uni_sz),
                BF.trade_low.name: to * np.random.uniform(0.999, 1.0, uni_sz),
                BF.trade_close.name: to * np.random.uniform(0.999, 1.001, uni_sz),
                BF.trade_volume.name: np.random.randint(1, 10000, uni_sz),
                # Vwap
                BF.vwap.name: ref_px * np.random.uniform(0.999, 1.001, uni_sz),
            }
        )

    # noinspection PyUnreachableCode
    return (
        pd.concat([_build_minute(m) for m in sess_minutes])
        .astype(StockBarsPandasSchema)
        .sort_values([BF.sid.name, BF.ts.name])
        .reset_index(drop=True)
    )


def worker_init():
    global ref_bars  # noqa: PLW0603
    ref_bars = generate_synthetic_stock_1m_bars_day()


# noinspection DuplicatedCode
def build_bars(
    dates: Union[Sequence[dt.date], pd.DatetimeIndex],
    output_dir: PathLikeT = DEFAULT_DS_ROOT_PATH,
    row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
    compression: Union[str, ParquetCompression] = DFAULT_PARQUET_COMPRESSION,
):
    global ref_bars  # noqa: PLW0602
    if ref_bars is None:
        raise ValueError("Reference bars not initialized")

    output_dir = Path(str(output_dir))
    compression = ParquetCompression[compression]
    output_dir = get_parquet_dataset_root(output_dir, row_group_size, compression)
    output_dir.mkdir(parents=True, exist_ok=True)

    LOG.info(
        f"Building bars from {dates[0]:%Y-%m-%d} to {dates[-1]:%Y-%m-%d}, saving to {output_dir}"
    )

    # noinspection DuplicatedCode
    def build_new_bar(new_date: dt.date) -> pd.DataFrame:
        new_bars = ref_bars
        schema = new_bars.dtypes
        excluded_cols = {BF.ticker.value, BF.sid.value}
        for c in new_bars:
            if c in excluded_cols:
                continue
            kind: str = schema[c].kind  # type: ignore[call-overload]
            if kind == "f":
                new_bars[c] = new_bars[c] * np.random.uniform(0.8, 1.2)
            elif kind == "i":
                new_bars[c] = (new_bars[c] * np.random.uniform(0.2, 5)).astype(
                    schema[c]  # type: ignore[call-overload]
                )
            elif kind == "M":
                new_bars[c] = new_date + (new_bars[c] - new_bars[c].dt.normalize())  # type: ignore[operator]
        return new_bars

    for d in dates:
        try:
            start_t = time.time()

            bars_df = build_new_bar(new_date=d)
            generated_time = time.time() - start_t

            fpth = (
                output_dir
                / f"year={d:%Y}"
                / f"date={d:%Y%m%d}"
                / f"{d:%Y%m%d}_equity_etf_1m_bars.pq"
            )
            fpth.parent.mkdir(parents=True, exist_ok=True)

            # noinspection PyTypeChecker
            bars_df.to_parquet(
                path=fpth,
                engine="pyarrow",
                compression=compression,
                index=False,
                row_group_size=row_group_size,
            )  # type: ignore[call-overload]
            LOG.info(
                f"Written [{row_group_size=} {compression=}]"
                f"[total={(time.time() - start_t):.2f}, generate={generated_time:.2f}) seconds]: "
                f"{fpth}"
            )
        except Exception as e:
            LOG.exception(f"FAILURE with date {d}: {e}")


def common(output_dir: PathLikeT = DEFAULT_DS_ROOT_PATH):
    output_dir = Path(str(output_dir))
    output_dir.mkdir(parents=True, exist_ok=True)

    global ref_bars  # noqa: PLW0602
    if ref_bars is None:
        worker_init()


def generate_synthetic_stock_1m_bars(
    from_t: DateLikeT = DEFAULT_START_T,
    to_t: DateLikeT = DEFAULT_END_T,
    output_dir: PathLikeT = DEFAULT_DS_ROOT_PATH,
    row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
    compression: Union[str, ParquetCompression] = DFAULT_PARQUET_COMPRESSION,
):
    common(output_dir=output_dir)
    dates = pd.bdate_range(
        pd.Timestamp(from_t).normalize(), pd.Timestamp(to_t).normalize()
    )
    build_bars(
        dates,
        output_dir=output_dir,
        row_group_size=row_group_size,
        compression=compression,
    )


def generate_concurrent_synthetic_stock_1m_bars(
    from_t: DateLikeT = DEFAULT_START_T,
    to_t: DateLikeT = DEFAULT_END_T,
    output_dir: PathLikeT = DEFAULT_DS_ROOT_PATH,
    num_workers: int = DEFAULT_NUM_WORKERS,
    row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
    compression: Union[str, ParquetCompression] = DFAULT_PARQUET_COMPRESSION,
):
    if (num_workers := max(1, int(num_workers))) == 1:
        return generate_synthetic_stock_1m_bars(
            from_t=from_t,
            to_t=to_t,
            output_dir=output_dir,
            row_group_size=row_group_size,
            compression=compression,
        )

    common(output_dir=output_dir)
    dates = pd.bdate_range(
        pd.Timestamp(from_t).normalize(), pd.Timestamp(to_t).normalize()
    )
    batch_size = int(len(dates) / num_workers) + 1
    batches = [dates[i: i + batch_size] for i in range(0, len(dates), batch_size)]

    with concurrent.futures.ProcessPoolExecutor(
        max_workers=num_workers,
        initializer=worker_init,
    ) as executor:
        # Prepare a list of futures
        futures = [
            executor.submit(
                build_bars,
                dates=batch_dates,
                output_dir=output_dir,
                row_group_size=row_group_size,
                compression=compression,
            )
            for batch_dates in batches
        ]

        # Iterate over futures to get their results
        for future in concurrent.futures.as_completed(futures):
            LOG.info(future.result())
