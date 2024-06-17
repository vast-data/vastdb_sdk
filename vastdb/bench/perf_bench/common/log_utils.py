import logging
from pathlib import Path
from threading import RLock
from typing import Optional, Union

from vastdb.bench.perf_bench.common.constants import LogLevel
from vastdb.bench.perf_bench.common.types import PathLikeT


class LogConfigError(Exception):
    pass


_logging_configured: bool = False
_log_level: int = logging.INFO
_log_file: Optional[Path] = None
_lock: RLock = RLock()


def set_log_file(log_file: PathLikeT) -> None:
    global _log_file  # noqa: PLW0603
    with _lock:
        if _logging_configured:
            raise LogConfigError(
                "Cannot change log file after logging has been configured."
            )

        log_file = Path(str(log_file))
        log_file.parent.mkdir(parents=True, exist_ok=True)
        log_file.touch(exist_ok=True)
        _log_file = log_file


def get_log_level() -> int:
    return _log_level


def set_log_level(level: Union[str, LogLevel, int]) -> None:
    global _log_level  # noqa: PLW0603
    if isinstance(level, str):
        level = LogLevel[level].to_int()
    with _lock:
        if level != _log_level and _logging_configured:
            raise LogConfigError(
                "Cannot change log level after logging has been configured."
            )
        _log_level = level


def get_logger(name: Optional[str]) -> logging.Logger:
    global _logging_configured  # noqa: PLW0603
    with _lock:
        if not _logging_configured:
            logging.basicConfig(
                level=_log_level,
                format=(
                    "%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s"
                ),
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            if _log_file:
                root_logger = logging.getLogger()
                fh = logging.FileHandler(_log_file, mode="a")
                fh.setFormatter(root_logger.handlers[0].formatter)
                root_logger.addHandler(fh)
            _logging_configured = True
    return logging.getLogger(name)
