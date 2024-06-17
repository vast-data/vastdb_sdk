# ruff: noqa: PLW2901, C901

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence

import pandas as pd

from vastdb.bench.perf_bench.common.constants import (
    DEFAULT_RESULTS_DIR,
)
from vastdb.bench.perf_bench.common.log_utils import get_logger
from vastdb.bench.perf_bench.common.types import PathLikeT
from vastdb.bench.perf_bench.orchestrate import results_helpers as bc
from vastdb.bench.perf_bench.orchestrate.bench_spec import BenchResult
from vastdb.bench.perf_bench.orchestrate.scenario import BenchScenario

LOG = get_logger(__name__)


@dataclass(frozen=True)
class RunnableBenchSession:
    bench_name: str
    scenarios: Sequence[BenchScenario]
    runs_per_bench: int
    parallelism: int
    results_base_dir: Optional[PathLikeT]


def run_scenarios(
    scenarios: Sequence[BenchScenario],
    runs_per_bench: int = 3,
    parallelism: int = 1,
    results_base_dir: Optional[str] = None,
):
    results_path = (
        Path(results_base_dir or DEFAULT_RESULTS_DIR)
        / f"results_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}_par{parallelism}.csv"
    )

    if (runs_per_bench := int(runs_per_bench)) < 1:
        raise ValueError(f"runs_per_bench must be >= 1: {runs_per_bench=}")
    if (parallelism := int(parallelism)) < 1:
        raise ValueError(f"parallelism must be >= 1: {parallelism=}")

    LOG.info("Will run '%d' scenarios", len(scenarios))

    # noinspection PyShadowingNames
    def worker_init():
        import numpy as np  # noqa: F401
        import pandas as pd  # noqa: F401
        import pyarrow as pa  # noqa: F401

        from vastdb.bench.perf_bench.common.utils import get_logger

        get_logger(__name__).info("Warmed up.")

    results: List[BenchResult] = []
    for i, scen in enumerate(scenarios):
        LOG.info(f"Running scenario: {scen.key} [{i + 1} of {len(scenarios)}]")
        scen.run(
            n_runs=runs_per_bench,
            discard_first_run=True,
            parallelism=parallelism,
            workers_init=worker_init,
        )
        results.extend(scen.results or ())
        bc.save_results(results, results_path=results_path)

    # Save the results from all workers and print stats
    bc.save_results(results, results_path=results_path)
    results_stats_df = bc.calculate_aggregate_stats(results=results)
    LOG.info(f"\n{results_stats_df.to_string()}")

    # Verify the results written
    results_df = bc.results_to_df(results)
    results_df2 = pd.read_csv(results_path).astype(results_df.dtypes)
    pd.testing.assert_frame_equal(results_df, results_df2)
