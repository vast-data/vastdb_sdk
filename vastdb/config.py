"""Configuration-related dataclasses."""

import logging
from dataclasses import dataclass, field
from typing import Callable, List, Optional

import backoff


@dataclass
class BackoffConfig:
    """Retry configuration."""

    wait_gen: Callable = field(default=backoff.expo)
    max_value: Optional[float] = None  # max duration for a single wait period
    max_tries: int = 10
    max_time: float = 60.0  # in seconds
    backoff_log_level: int = logging.DEBUG


@dataclass
class QueryConfig:
    """Query execution configiration."""

    # allows server-side parallel processing by issuing multiple reads concurrently for a single RPC
    num_sub_splits: int = 4

    # used to split the table into disjoint subsets of rows, to be processed concurrently using multiple RPCs
    # will be estimated from the table's row count, if not explicitly set
    num_splits: Optional[int] = None

    # each endpoint will be handled by a separate worker thread
    # a single endpoint can be specified more than once to benefit from multithreaded execution
    data_endpoints: Optional[List[str]] = None

    # a subsplit fiber will finish after sending this number of rows back to the client
    limit_rows_per_sub_split: int = 128 * 1024

    # each fiber will read the following number of rowgroups coninuously before skipping
    # in order to use semi-sorted projections this value must be 8 (this is the hard coded size of a row groups per row block).
    num_row_groups_per_sub_split: int = 8

    # can be disabled for benchmarking purposes
    use_semi_sorted_projections: bool = True

    # enforce using a specific semi-sorted projection (if enabled above)
    semi_sorted_projection_name: Optional[str] = None

    # used to estimate the number of splits, given the table rows' count
    rows_per_split: int = 4000000

    # used for worker threads' naming
    query_id: str = ""

    # non-negative integer, used for server-side prioritization of queued requests:
    # - requests with lower values will be served before requests with higher values.
    # - if unset, the request will be added to the queue's end.
    queue_priority: Optional[int] = None


@dataclass
class ImportConfig:
    """Import execution configiration."""

    import_concurrency: int = 2

    # import key column names
    key_names: Optional[List[str]] = None
