from pathlib import Path
from typing import Annotated, List, Optional

import pandas as pd
import typer

from vastdb.bench.perf_bench.common.constants import (
    DEFAULT_END_T,
    DEFAULT_RESULTS_DIR,
    DEFAULT_START_T,
    DFAULT_PARQUET_COMPRESSION,
    LOCAL_FS_DS_PATH,
    LogLevel,
    ParquetCompression,
)
from vastdb.bench.perf_bench.common.log_utils import (
    get_logger,
    set_log_file,
    set_log_level,
)
from vastdb.bench.perf_bench.common.utils import getenv_flag, load_module_from_path
from vastdb.bench.perf_bench.dataset.generate_secmaster import (
    SM_PATH,
    generate_secmaster,
)
from vastdb.bench.perf_bench.dataset.generate_stocks_dataset import (
    generate_concurrent_synthetic_stock_1m_bars,
)
from vastdb.bench.perf_bench.orchestrate.scenario import BenchScenario
from vastdb.bench.perf_bench.run import run_scenarios

app = typer.Typer(pretty_exceptions_enable=getenv_flag("TYPER_PRETTY_EXCEPTIONS"))

_MY_DIR = Path(__file__).parent


# noinspection PyUnusedLocal
@app.callback()
def cli_common(
    ctx: typer.Context,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            is_flag=True,
        ),
    ] = False,
    log_level: Annotated[
        Optional[LogLevel],
        typer.Option(
            "--log-level",
            case_sensitive=False,
        ),
    ] = None,
    log_file: Annotated[
        Optional[Path],
        typer.Option(
            "--log-file",
            writable=True,
            file_okay=True,
            dir_okay=False,
            resolve_path=True,
        ),
    ] = None,
):
    if verbose:
        log_level = LogLevel.DEBUG
    if log_level:
        set_log_level(log_level)
    if log_file:
        set_log_file(log_file)
    get_logger(__name__).info("CLI common setup done.")


def _positive_int(value: str) -> int:
    i_value = int(value)
    if i_value <= 0:
        raise typer.BadParameter(f"Must be a positive integer: {value}.")
    return i_value


# noinspection PyUnusedLocal
@app.command()
def run_bench(
    ctx: typer.Context,
    bench_name: Annotated[
        str,
        typer.Option(
            "--bench-name",
        ),
    ],
    parallelism: Annotated[
        List[int],
        typer.Option(
            "--parallelism",
            callback=lambda par: [_positive_int(p) for p in par],
        ),
    ],
    runs_per_bench: Annotated[
        int,
        typer.Option(
            "--runs-per-bench",
            callback=_positive_int,
        ),
    ] = 3,
    bench_generator_path: Annotated[
        Path,
        typer.Option(
            "--bench-generator-path",
            readable=True,
            file_okay=True,
            dir_okay=False,
            resolve_path=True,
        ),
    ] = _MY_DIR / "bench_repo" / "mega_combo.py",
    results_base_dir: Annotated[
        Path,
        typer.Option(
            "--log-file",
            writable=True,
            file_okay=False,
            dir_okay=True,
            resolve_path=True,
        ),
    ] = DEFAULT_RESULTS_DIR,
):
    if not (bench_name := bench_name.strip()):
        raise typer.BadParameter("Bench name must be non-empty.")

    mod = load_module_from_path(bench_generator_path)
    scenarios: List[BenchScenario] = mod.build_scenarios(base_key=bench_name)
    for para in parallelism:
        run_scenarios(
            scenarios=scenarios,
            runs_per_bench=runs_per_bench,
            parallelism=para,
            results_base_dir=str(results_base_dir),
        )


# noinspection PyUnusedLocal
@app.command()
def build_secmaster(
    ctx: typer.Context,
):
    generate_secmaster()


# noinspection PyUnusedLocal
@app.command()
def build_dataset(
    ctx: typer.Context,
    start_date: Annotated[
        str,
        typer.Option(
            "--start-date",
            help="Start date for the dataset.",
            callback=lambda d: pd.Timestamp(d).normalize(),
        ),
    ] = DEFAULT_START_T.strftime("%Y%m%d"),
    end_date: Annotated[
        str,
        typer.Option(
            "--end-date",
            help="Start date for the dataset.",
            callback=lambda d: pd.Timestamp(d).normalize(),
        ),
    ] = DEFAULT_END_T.strftime("%Y%m%d"),
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            writable=True,
            file_okay=False,
            dir_okay=True,
            resolve_path=True,
        ),
    ] = LOCAL_FS_DS_PATH,
    parallelism: Annotated[
        int,
        typer.Option(
            "--parallelism",
            callback=_positive_int,
        ),
    ] = 6,
    row_group_size: Annotated[
        int,
        typer.Option(
            "--row-group-size",
            callback=_positive_int,
            help=(
                "Row group size for the dataset, some common values are: 64 * 1024, 128 * 1024, 256"
                " * 1024, 512 * 1024,1024 * 1024, 1.5 * 1024 * 1024."
            ),
        ),
    ] = 256 * 1024,
    compression: Annotated[
        ParquetCompression,
        typer.Option(
            "--compression",
            help="Parquet compression algorithm.",
        ),
    ] = DFAULT_PARQUET_COMPRESSION,
):
    if row_group_size < 1024:
        raise typer.BadParameter("Row group size must be at least 1024.")
    if parallelism < 1:
        raise typer.BadParameter("Parallelism must be at least 1.")
    if start_date > end_date:
        raise typer.BadParameter("Start date must be before the end date.")
    if not SM_PATH.is_file():
        generate_secmaster()
    generate_concurrent_synthetic_stock_1m_bars(
        from_t=start_date,
        to_t=end_date,
        output_dir=output_dir,
        num_workers=parallelism,
        row_group_size=row_group_size,
        compression=compression,
    )


if __name__ == "__main__":
    # Set the metadata only if we execute the main (not on just importing this module)
    app()
