from pathlib import Path
from typing import List, Optional, Union

import numpy as np
import pandas as pd

from vastdb.bench.perf_bench.common.log_utils import get_logger
from vastdb.bench.perf_bench.orchestrate.bench_spec import BenchResult

LOG = get_logger(__name__)

PARAM_NAMES_SEP = "|"


def results_to_df(
    results: List[BenchResult],
) -> pd.DataFrame:
    results_df = pd.DataFrame([r.to_dict() for r in results])
    results_df["param_names"] = PARAM_NAMES_SEP.join(
        [c for c in (results[0].params or ()) if c in results_df]
    )
    return results_df


def save_results(
    results: List[BenchResult],
    results_path: Union[str, Path],
    append: bool = False,
):
    if not results:
        return

    results_path = Path(results_path)
    results_path.parent.mkdir(parents=True, exist_ok=True)
    results_df = results_to_df(results)

    if append and results_path.is_file():
        prev_results = pd.read_csv(results_path).astype(results_df.dtypes)
        results_df = pd.concat([prev_results, results_df], ignore_index=True)

    results_df.to_csv(results_path, index=False)
    LOG.info(f"Results written to {results_path}")


def calc_total_time_coverage_seconds(results_df: pd.DataFrame) -> float:
    # Step 1: Separate start and end times, tagging them differently
    starts = results_df["start_ts"].rename("time").to_frame().assign(event=1)
    ends = results_df["end_ts"].rename("time").to_frame().assign(event=-1)

    # Combine and sort chronologically
    timeline = pd.concat([starts, ends]).sort_values("time")

    # Step 2 & 3: Calculate cumulative sum to identify active intervals
    timeline["active_intervals"] = timeline["event"].cumsum()

    # Identify transitions (changes in active_intervals)
    timeline["change"] = timeline["active_intervals"].diff().fillna(1).astype(bool)

    # Calculate durations between changes
    timeline["duration"] = timeline["time"].diff().shift(-1).fillna(pd.Timedelta(0))

    # Only consider periods where at least one interval is active
    total_coverage: pd.Timedelta = timeline.query("active_intervals > 0")[
        "duration"
    ].sum()
    return total_coverage.total_seconds()


def calculate_aggregate_stats(
    results: Optional[List[BenchResult]] = None,
    results_df: Optional[pd.DataFrame] = None,
    results_path: Union[None, str, Path] = None,
) -> pd.DataFrame:
    if sum(bool(x) for x in (results, results_df, results_path)) != 1:
        raise ValueError(
            "Exactly one of results, results_df, or results_path must be provided"
        )

    if results:
        r_df = results_to_df(results)
        group_flds = list(results[0].params or ())
    else:
        r_df: pd.DataFrame = (  # type: ignore[assignment,no-redef]
            pd.read_csv(results_path) if results_path else results_df
        )
        p_names: pd.Series = results_df["param_names"]  # type: ignore[index]
        group_flds = list(p_names.values[0].split(PARAM_NAMES_SEP))
    group_flds = ["key", "n_cols", *group_flds]

    if not group_flds:
        raise ValueError("No group fields found")

    def _get_agg(col: str) -> str:
        mapper = {
            "start_ts": "min",
            "end_ts": "max",
            "round": "max",
            "duration_sec": "mean",
        }
        return mapper.get(
            col, "sum" if np.issubdtype(r_df.dtypes[col], np.number) else "last"
        )

    non_group_flds = [c for c in r_df.columns if c not in group_flds]
    agg_df = (
        r_df.groupby(group_flds)
        .aggregate(
            {c: _get_agg(c) for c in non_group_flds},
            total=("duration_sec", "count"),
        )
        .drop(columns=["pid", "host", "param_names", "round", "duration_sec"])
        .sort_index()
    )
    agg_df["duration_sec"] = (
        r_df.groupby(group_flds)
        .apply(calc_total_time_coverage_seconds, include_groups=False)
        .sort_index()
    )
    agg_df["M_rows_per_sec"] = (agg_df["n_rows"] / agg_df["duration_sec"] / 1e6).astype(
        "float64"
    )
    agg_df["MB_per_sec"] = (
        agg_df["n_bytes"] / 1024 ** 2 / agg_df["duration_sec"]
    ).astype("float64")

    return agg_df
