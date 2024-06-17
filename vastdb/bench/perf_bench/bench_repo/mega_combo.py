from typing import List, Optional

from vastdb.bench.perf_bench.common.constants import (
    LOCAL_FS_DS_PATH,
    NFS_DS_PATH,  # noqa: F401
    S3_DS_PATH,  # noqa: F401
    ParquetCompression,
    VastConnDetails,
)
from vastdb.bench.perf_bench.dataset.schemas import DEFAULT_BARS_COLUMNS
from vastdb.bench.perf_bench.orchestrate.scenario import BenchScenario
from vastdb.bench.perf_bench.orchestrate.scenario_generator import (
    generate_perf_bench_scenarios,
)
from vastdb.bench.perf_bench.query.query import QueryBackend


def build_scenarios(
    base_key: str,
    conn_details: Optional[VastConnDetails] = None,
) -> List[BenchScenario]:
    return generate_perf_bench_scenarios(
        base_key=base_key,
        conn_details=conn_details or VastConnDetails(),
        query_backends=[
            QueryBackend.pyarrow,
            # QueryBackend.vastdb,
        ],
        columns_choices=(DEFAULT_BARS_COLUMNS,),
        universe_choices=(
            "Single",
            "Tiny",
            "SmallSeq",
            "Medium",
            "Medium2",
            "Large",
        ),
        num_bdays=[
            1,  # 1d
            5,  # 1w
            # 22,  # 1m
            65,  # 3m
            # 130,  # 6m
            252,  # 1y
        ],

        # Arrow-specific options
        fs_path_choices=[
            # NFS_DS_PATH,
            LOCAL_FS_DS_PATH,
            # S3_DS_PATH,
        ],
        rowgroup_size_choices=[  # make sure you have previously generated the respective datasets
            # 64 * 1024,
            # 128 * 1024,
            256 * 1024,
            # 512 * 1024,
            # DEFAULT_ROW_GROUP_SIZE,
            # int(1.5 * 1024 * 1024),
        ],
        compression_choices=[
            ParquetCompression.LZ4,
        ],
        arrow_batching_spec_choices=[
            # {"batch_size": 2*2**16, "batch_readahead": 16, "fragment_readahead": 4},
            # {"batch_size": 6*2**16, "batch_readahead": 12, "fragment_readahead": 4},
            # DEFAULT_ARROW_KWARGS,
            {"batch_size": 16 * 2 ** 16, "batch_readahead": 16, "fragment_readahead": 4},
            # {"batch_size": 24 * 2 ** 16, "batch_readahead": 12, "fragment_readahead": 4},
            # {"batch_size": 32*2**16, "batch_readahead": 12, "fragment_readahead": 4},
            # {"batch_size": 64*2**16, "batch_readahead": 12, "fragment_readahead": 4},
            # {"batch_size": 128*2**16, "batch_readahead": 12, "fragment_readahead": 4},
        ],

        # VastDB-specific options
        vdb_num_sub_splits_choices=(
            # 1,  # Default
            # 4,
            8,
            # 16,
        ),
        vdb_num_row_groups_per_sub_split_choices=(
            # 1,
            # 4,
            8,  # Default
        ),
    )
