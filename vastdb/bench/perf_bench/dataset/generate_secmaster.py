from pathlib import Path
from typing import NamedTuple, TypedDict

import requests

from vastdb.bench.perf_bench.common.log_utils import get_logger

_MY_DIR = Path(__file__).parent
SM_PATH = _MY_DIR / "secmaster.py"

LOG = get_logger(__name__)


class NasdaqRawRecord(TypedDict):
    symbol: str
    name: str
    lastsale: str
    netchange: str
    pctchange: str
    marketCap: str
    url: str


class NasdaqRecord(NamedTuple):
    symbol: str
    id: int
    last_sale: float

    @staticmethod
    def id_from_ticker(ticker: str) -> int:
        base = 100
        max_width = 5
        offset = ord(" ")
        if len(ticker := ticker.strip()) > max_width:
            raise ValueError(f"Ticker too long: {ticker}")
        ticker = ticker.rjust(max_width, " ")
        return sum(
            (ord(c) - offset) * base ** (len(ticker) - i)
            for i, c in enumerate(ticker.upper())
        )

    @classmethod
    def from_raw_dict(cls, raw_dict: NasdaqRawRecord) -> "NasdaqRecord":
        return cls(
            symbol=(sym := raw_dict["symbol"].strip().upper()),
            id=cls.id_from_ticker(sym),
            last_sale=float(raw_dict["lastsale"].replace("$", "").replace(",", "")),
        )


def generate_secmaster():
    resp = requests.get(
        "http://api.nasdaq.com/api/screener/stocks?limit=3000",
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
    )
    resp.raise_for_status()
    resp_j = resp.json()
    records = [
        NasdaqRecord.from_raw_dict(r)
        for r in resp_j["data"]["table"]["rows"]
        if r and r["symbol"].strip()
    ]
    _ticker_to_sid = {r.symbol: r.id for r in records}
    _sid_to_ticker = {r.id: r.symbol for r in records}
    _indicative_px = {r.id: r.last_sale for r in records}

    uni = sorted(_ticker_to_sid)
    secm_file_contents = f"""
_ticker_to_sid = {_ticker_to_sid}


_sid_to_ticker = {_sid_to_ticker}


_indicative_px = {_indicative_px}


def to_sid(ticker: str) -> int:
    return _ticker_to_sid[ticker]


def to_ticker(sid: int) -> str:
    return _sid_to_ticker[sid]


def get_indicative_px(sid: int) -> float:
    return _indicative_px[sid]


UNI_SPEC = {{
    "Large": (large_uni := {uni}),
    "Single": large_uni[:1],
    "Tiny": (tiny_uni := large_uni[1::50]),
    "Small": (small_uni := large_uni[1::10]),
    "SmallSeq": large_uni[: len(small_uni)],
    "Medium": (med_uni := large_uni[1::6]),
    "MediumSeq": large_uni[: (len(med_uni))],
    "Medium2": (med2_uni := large_uni[1::8]),
    "Medium2Seq": large_uni[: (len(med2_uni))],
}}
"""
    with open(SM_PATH, "w") as f:
        f.write(secm_file_contents)

    LOG.info("Secmaster generated (total stocks: %d): %s", len(uni), SM_PATH)
