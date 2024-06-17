import datetime as dt
import os
from typing import Dict, List, NamedTuple, Optional, Union

import pandas as pd

DEFAULT_CSV_SEPARATOR = ","


class BenchResult(NamedTuple):
    # The identifier for this result
    key: str
    round: int

    # Host details and time
    host: str
    pid: int

    # Results details
    start_ts: pd.Timestamp
    end_ts: pd.Timestamp
    duration_sec: float
    n_rows: int
    n_cols: int
    n_bytes: int

    params: Optional[Dict[str, Union[str, int, float]]] = None

    def to_dict(
        self,
        include_params: bool = True,
        flatten_params: bool = True,
    ) -> Dict[str, Union[str, int, float]]:
        dc = {f: getattr(self, f) for f in self._fields if f != "params"}
        if include_params:
            params = self.params or {}
            if flatten_params:
                dc = {**dc, **{pk: pv for pk, pv in params.items() if pk not in dc}}
            else:
                dc["params"] = params
        return dc

    def fields(
        self,
        include_params: bool = True,
        flatten_params: bool = True,
    ) -> List[str]:
        return list(
            self.to_dict(include_params=include_params, flatten_params=flatten_params)
        )

    def csv_header(
        self,
        separator: str = DEFAULT_CSV_SEPARATOR,
        include_params: bool = True,
    ) -> str:
        return separator.join(
            self.to_dict(include_params=include_params, flatten_params=True)
        )

    def to_csv(
        self,
        separator: str = DEFAULT_CSV_SEPARATOR,
        include_params: bool = True,
        include_header: bool = False,
    ) -> str:
        def _to_str(v) -> str:
            if isinstance(v, dt.date):
                v = v.isoformat()
            v_str = str(v) if not isinstance(v, str) else v
            if separator in v_str:
                if '"' in v_str:
                    raise ValueError(f"Can't handle double quotes in value: {v_str}")
                v_str = f'"{v_str}"'
            return v_str

        data = [
            _to_str(d)
            for d in self.to_dict(include_params=include_params, flatten_params=True)
        ]
        data_csv = separator.join(data)

        if include_header:
            data_csv = os.linesep.join(
                [
                    self.csv_header(separator=separator, include_params=include_params),
                    data_csv,
                ]
            )

        return data_csv
