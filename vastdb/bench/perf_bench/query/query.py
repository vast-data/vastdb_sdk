import datetime as dt
import logging
from typing import Any, Dict, Optional, Protocol, Sequence

import pyarrow as pa

from vastdb.bench.perf_bench.common.types import StrEnum


class QueryRunner(Protocol):
    def __call__(
        self,
        universe: Sequence[str],
        columns: Optional[Sequence[str]],
        from_t: Optional[dt.datetime],
        to_t: Optional[dt.datetime],
        path: Optional[str],
        use_sid: bool,
        logger: Optional[logging.Logger],
        backend_kwargs: Optional[Dict[str, Any]],
    ) -> pa.Table:
        ...


class QueryBackend(StrEnum):
    vastdb = "vastdb"
    pyarrow = "pyarrow"

    @property
    def _query_fun(self) -> QueryRunner:
        if self is QueryBackend.vastdb:
            from vastdb.bench.perf_bench.query.query_vastdb import query_vastdb

            return query_vastdb
        elif self is QueryBackend.pyarrow:
            from vastdb.bench.perf_bench.query.query_pyarrow import query_pyarrow

            return query_pyarrow
        raise NotImplementedError(f"Unsupported query backend: {self}")

    def run_query(self, *args, **kwargs) -> pa.Table:
        return self._query_fun(*args, **kwargs)
