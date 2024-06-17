import concurrent
import os
import socket
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pandas as pd

from vastdb.bench.perf_bench.orchestrate.bench_spec import BenchResult
from vastdb.bench.perf_bench.orchestrate.results_helpers import LOG


@dataclass
class BenchScenario:
    key: str
    func: Callable
    kwargs: Dict[str, Any]
    results: Optional[Tuple[BenchResult, ...]] = tuple()
    default_result_params: Optional[Dict[str, Union[str, int, float]]] = None

    # noinspection PyUnresolvedReferences
    def run(
        self,
        n_runs: int = 1,
        discard_first_run: bool = True,
        parallelism: int = 1,
        workers_init: Optional[Callable] = None,
    ):
        if parallelism == 1:
            self.results = tuple(
                self._do_run(n_runs=n_runs, discard_first_run=discard_first_run)
            )
        elif parallelism > 1:
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=parallelism,
                initializer=workers_init,
            ) as executor:
                futures = [
                    executor.submit(
                        self._do_run, n_runs=n_runs, discard_first_run=discard_first_run
                    )
                    for _ in range(parallelism)
                ]

                # Iterate over futures to get their results
                results = []
                for future in concurrent.futures.as_completed(futures):
                    LOG.info("Future completed")
                    results.extend(future.result())
                self.results = tuple(results)
        else:
            raise ValueError("Can't use a negative number of parallel benches")

    def _do_run(
        self,
        n_runs: int = 1,
        discard_first_run: bool = True,
    ) -> List[BenchResult]:
        hostname = socket.gethostname()
        pid = os.getpid()
        results = []
        kwargs = self.kwargs or {}

        for i in range(n_runs + int(discard_first_run)):
            key = self.key

            start_ts = time.time_ns()
            ret_v = self.func(**kwargs)
            end_ts = time.time_ns()

            if discard_first_run and i == 0:
                LOG.info(f"Discarding first run ({key})")
                continue

            n_bytes, n_rows, n_cols = 0, 0, 0

            if hasattr(ret_v, "shape"):
                shape = ret_v.shape
                if len(shape):
                    n_rows = shape[0]
                if len(shape) > 1:
                    n_cols = shape[1]

            if hasattr(ret_v, "nbytes"):
                n_bytes = ret_v.nbytes
            elif hasattr(ret_v, "memory_usage"):
                n_bytes = ret_v.memory_usage().sum()
            elif hasattr(ret_v, "estimated_size"):
                n_bytes = ret_v.estimated_size()

            result = BenchResult(
                key=key,
                round=i,
                host=hostname,
                pid=pid,
                start_ts=pd.Timestamp(start_ts, unit="ns"),
                end_ts=pd.Timestamp(end_ts, unit="ns"),
                duration_sec=(end_ts - start_ts) / 1e9,
                n_rows=n_rows,
                n_cols=n_cols,
                n_bytes=n_bytes,
                params=self.default_result_params or {},
            )
            results.append(result)

            LOG.info(f"{result.duration_sec} sec")

        return results
