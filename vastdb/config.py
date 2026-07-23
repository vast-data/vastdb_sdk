"""Configuration-related dataclasses."""

import logging
from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import TYPE_CHECKING, Callable, List, Optional

import backoff

from vastdb._session_context import SessionContext

if TYPE_CHECKING:
    from .transaction import Transaction


@dataclass
class BackoffConfig:
    """Retry configuration."""

    wait_gen: Callable = field(default=backoff.expo)
    max_value: Optional[float] = None  # max duration for a single wait period
    max_tries: int = 10
    max_time: float = 60.0  # in seconds
    backoff_log_level: int = logging.DEBUG


@dataclass(frozen=True)
class SessionConfig:
    """Session Defaults Config."""

    data_endpoints: list[str] | None

    def __post_init__(self):
        """Post init."""
        if self.data_endpoints is not None and len(self.data_endpoints) == 0:
            raise ValueError("data_endpoints cannot be empty")


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

    def override_missing(self, session_context: SessionContext) -> "QueryConfig":
        """With session defaults."""
        return replace(
            self,
            data_endpoints=self.data_endpoints if self.data_endpoints is not None else session_context.data_endpoints
        )

    def require_endpoints(self) -> list[str]:
        """Require endpoints throw otherwise."""
        if self.data_endpoints is None:
            raise ValueError("data_endpoints is None")

        return self.data_endpoints


@dataclass
class ImportConfig:
    """Import execution configiration."""

    import_concurrency: int = 2

    # each endpoint will be handled by a separate import thread
    data_endpoints: Optional[List[str]] = None

    # import key column names
    key_names: Optional[List[str]] = None

    def __post_init__(self):
        """Post init."""
        if self.data_endpoints is not None and len(self.data_endpoints) == 0:
            raise ValueError("data_endpoints cannot be empty if provided")

    def override_missing(self, session_context: SessionContext) -> "ImportConfig":
        """With session defaults."""
        return replace(
            self,
            data_endpoints=self.data_endpoints if self.data_endpoints is not None else session_context.data_endpoints
        )

    def require_endpoints(self) -> list[str]:
        """Require endpoints throw otherwise."""
        if self.data_endpoints is None:
            raise ValueError("data_endpoints is None")

        return self.data_endpoints


@dataclass(frozen=True)
class BufferedInsertConfig:
    """Flush configuration for a by partition buffered inserter."""

    # Max size in bytes of the data the buffer can hold at a give time
    capacity_in_bytes: int = 10 * 1024 * 1024

    # Time interval after which data will be flushed to server, regardless of anything else
    timeout: Optional[timedelta] = None

    # If set, will be used for the entire buffered insert process, otherwise each flush will occur in a separate transaction
    tx: Optional["Transaction"] = None

    # The minimum required number of rows buffered in some partition in order to flush it
    # If the buffer is out of capacity and this target is not reached yet, some flush will occur anyway (same for timer)
    target_num_rows_per_partition: int = 2 ** 16

    # if not supplied the default endpoint/s will be used
    # single endpoint can be specified more than once
    data_endpoints: list[str] = field(default_factory=list)

    def __post_init__(self):
        """Validate params."""
        if self.capacity_in_bytes <= 0:
            raise ValueError("Capacity in bytes must be a positive integer")

        if self.tx is not None and not self.tx.is_active:
            raise ValueError("Provided transaction must be active")

        if self.timeout is not None and self.timeout.total_seconds() <= 0:
            raise ValueError("Timeout must be a positive duration.")

        if self.target_num_rows_per_partition <= 0:
            raise ValueError("Target number of rows per partition must be a positive integer")

        if self.tx is not None and self.timeout is not None:
            raise ValueError("Cannot set a timeout when an explicit transaction is provided")
