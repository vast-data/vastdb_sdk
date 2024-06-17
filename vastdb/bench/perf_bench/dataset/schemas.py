from functools import lru_cache
from types import MappingProxyType
from typing import Dict, Mapping, Union

import numpy as np
import pyarrow as pa
from pandas.core.dtypes.base import ExtensionDtype

from vastdb.bench.perf_bench.common.types import StrEnum

PandasDTypeT = Union[ExtensionDtype, np.dtype]


class StockBarField(StrEnum):
    sid = "sid"
    ts = "ts"
    ticker = "ticker"

    # Ask
    ask_open = "ask_open"
    ask_high = "ask_high"
    ask_low = "ask_low"
    ask_close = "ask_close"
    ask_qty = "ask_qty"

    # Bid
    bid_open = "bid_open"
    bid_high = "bid_high"
    bid_low = "bid_low"
    bid_close = "bid_close"
    bid_qty = "bid_qty"

    # Trades
    trade_open = "trade_open"
    trade_high = "trade_high"
    trade_low = "trade_low"
    trade_close = "trade_close"
    trade_volume = "trade_volume"

    # VWAP
    vwap = "vwap"

    @property
    def pa_type(self) -> pa.DataType:
        return _get_field_pyarrow_types()[self]

    @property
    def pd_type(self) -> PandasDTypeT:
        return self.pa_type.to_pandas_dtype()


BF = StockBarField


@lru_cache
def _get_field_pyarrow_types() -> Mapping[str, pa.DataType]:
    return MappingProxyType(
        {
            BF.sid.value: pa.int64(),
            BF.ts.value: pa.timestamp(unit="ns"),
            BF.ticker.value: pa.string(),
            BF.ask_open.value: pa.float64(),
            BF.ask_high.value: pa.float64(),
            BF.ask_low.value: pa.float64(),
            BF.ask_close.value: pa.float64(),
            BF.ask_qty.value: pa.int64(),
            BF.bid_open.value: pa.float64(),
            BF.bid_high.value: pa.float64(),
            BF.bid_low.value: pa.float64(),
            BF.bid_close.value: pa.float64(),
            BF.bid_qty.value: pa.int64(),
            BF.trade_open.value: pa.float64(),
            BF.trade_high.value: pa.float64(),
            BF.trade_low.value: pa.float64(),
            BF.trade_close.value: pa.float64(),
            BF.trade_volume.value: pa.int64(),
            BF.vwap.value: pa.float64(),
        }
    )


DEFAULT_BARS_COLUMNS = (
    BF.sid.value,
    BF.ts.value,
    BF.ask_open.value,
    BF.ask_close.value,
    BF.bid_open.value,
    BF.bid_close.value,
    BF.bid_qty.value,
)

# noinspection PyUnresolvedReferences
StockBarsArrowSchema: pa.Schema = pa.schema(
    (fld.value, fld.pa_type) for fld in StockBarField
)
# noinspection PyUnresolvedReferences
StockBarsPandasSchema: Dict[str, PandasDTypeT] = {
    fld.value: fld.pd_type for fld in StockBarField
}

BarsSortFields = (BF.sid.value, BF.ts.value)
