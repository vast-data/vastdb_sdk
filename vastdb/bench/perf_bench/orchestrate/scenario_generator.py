import datetime as dt
from itertools import product
from typing import Dict, List, Optional, Sequence, Union

import pandas as pd

from vastdb.bench.perf_bench.common.constants import (
    DEFAULT_ARROW_KWARGS,
    DEFAULT_ROW_GROUP_SIZE,
    DEFAULT_START_T,
    DFAULT_PARQUET_COMPRESSION,
    ParquetCompression,
    VastConnDetails,
)
from vastdb.bench.perf_bench.common.types import PathLikeT
from vastdb.bench.perf_bench.common.utils import get_parquet_dataset_root, infer_fs_type
from vastdb.bench.perf_bench.dataset import secmaster as sm
from vastdb.bench.perf_bench.dataset.schemas import DEFAULT_BARS_COLUMNS
from vastdb.bench.perf_bench.orchestrate.scenario import BenchScenario
from vastdb.bench.perf_bench.query.query import QueryBackend


def generate_perf_bench_scenarios(
    base_key: str,
    conn_details: VastConnDetails,
    # Common parameters
    query_backends: Sequence[Union[str, QueryBackend]] = (QueryBackend.pyarrow,),
    universe_choices: Sequence[Union[str, Sequence[str]]] = ("Single",),
    columns_choices: Sequence[Sequence[str]] = (DEFAULT_BARS_COLUMNS,),
    dt_start_t: Union[dt.date, str] = DEFAULT_START_T,
    num_bdays: Sequence[int] = (1,),
    # Arrow-specific options
    fs_path_choices: Optional[Sequence[PathLikeT]] = None,
    rowgroup_size_choices: Optional[Sequence[int]] = None,
    compression_choices: Optional[Sequence[Union[str, ParquetCompression]]] = None,
    arrow_batching_spec_choices: Optional[Sequence[Dict[str, int]]] = None,
    # VastDB-specific options
    vdb_num_sub_splits_choices: Optional[Sequence[int]] = None,
    vdb_num_row_groups_per_sub_split_choices: Optional[Sequence[int]] = None,
) -> List[BenchScenario]:
    if not base_key:
        raise ValueError("base_key must be provided")

    uni_choices: List[List[str]] = []
    for k in universe_choices:
        if isinstance(k, str):
            uni_choices.append(sm.UNI_SPEC.get(k, [k]))
        else:
            uni_choices.append(list(k))

    columns_choices = [[c for c in cs if c] for cs in columns_choices if cs]
    dt_start_t = pd.Timestamp(dt_start_t)
    dt_range_choices = [
        (dt_start_t, dt_start_t + pd.tseries.offsets.BDay(d))
        for d in sorted(set(num_bdays))
        if d > 0
    ]

    scenarios = []
    for i, (  # type: ignore[misc]
        uni,
        columns,
        (from_t, to_t),
        path,
        rowgroup_size,
        compression,
        arrow_batch_spec,
        vdb_num_sub_splits,
        vdb_num_row_groups_per_sub_split,
        query_backend,
    ) in enumerate(
        product(
            uni_choices,
            columns_choices,
            dt_range_choices,
            # Arrow specific options
            (*(fs_path_choices or []), None),
            (*(rowgroup_size_choices or [DEFAULT_ROW_GROUP_SIZE]), None),
            (*(compression_choices or [DFAULT_PARQUET_COMPRESSION]), None),
            (*(arrow_batching_spec_choices or [DEFAULT_ARROW_KWARGS]), None),
            # VastDB specific options
            (*(vdb_num_sub_splits_choices or [1]), None),
            (*(vdb_num_row_groups_per_sub_split_choices or [8]), None),
            [QueryBackend[qb] for qb in query_backends],
        )
    ):
        if query_backend is QueryBackend.vastdb:
            if not all((vdb_num_sub_splits, vdb_num_row_groups_per_sub_split)) or any(
                (path, rowgroup_size, compression, arrow_batch_spec)
            ):
                # ignore path and rg size for vastdb runs
                continue
            backend_kwargs = {
                "num_sub_splits": vdb_num_sub_splits,
                "num_row_groups_per_sub_split": vdb_num_row_groups_per_sub_split,
                "conn_details": conn_details,
            }
            path = None
        elif query_backend is QueryBackend.pyarrow:
            if not all((path, rowgroup_size, compression, arrow_batch_spec)) or any(
                (vdb_num_sub_splits, vdb_num_row_groups_per_sub_split)
            ):
                # ignore non-vastdb runs without a path or rg size
                continue
            backend_kwargs: Dict[str, Union[str, int, VastConnDetails]] = arrow_batch_spec  # type: ignore[no-redef]
            backend_kwargs["conn_details"] = conn_details
            path = get_parquet_dataset_root(path, rowgroup_size, compression)  # type: ignore[arg-type]
        else:
            raise NotImplementedError(f"Unsupported query backend: {query_backend=!r}")

        scen = BenchScenario(
            key=f"{base_key}_{query_backend.name}",
            func=query_backend.run_query,
            kwargs=dict(
                universe=uni,
                columns=columns,
                from_t=from_t,
                to_t=to_t,
                path=path,
                backend_kwargs=backend_kwargs,
            ),
            default_result_params={
                "uni_sz": len(uni),  # type: ignore
                "num_days": (to_t - from_t).days,  # type: ignore
                "query_backend": query_backend.name,  # type: ignore
                "fs_type": infer_fs_type(path or ""),  # type: ignore
                "rowgroup_size": rowgroup_size or -1,  # type: ignore
                "arrow_fragment_readahead": (  # type: ignore
                    arrow_batch_spec["fragment_readahead"] if arrow_batch_spec else -1  # type: ignore
                ),
                "arrow_batch_readahead": (  # type: ignore
                    arrow_batch_spec["batch_readahead"] if arrow_batch_spec else -1  # type: ignore
                ),
                "arrow_batch_size": (  # type: ignore
                    arrow_batch_spec["batch_size"] if arrow_batch_spec else -1  # type: ignore
                ),
                "vdb_num_sub_splits": vdb_num_sub_splits or -1,  # type: ignore
                "vdb_num_row_groups_per_sub_split": vdb_num_row_groups_per_sub_split or -1,  # type: ignore
            },
        )

        scenarios.append(scen)

    return scenarios
