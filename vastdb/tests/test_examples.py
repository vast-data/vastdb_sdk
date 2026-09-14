"""Run examples/*.py as integration checks against a live cluster."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from vastdb import errors
from vastdb.session import Session

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"
EXAMPLE_SCRIPTS = sorted(
    p for p in EXAMPLES_DIR.glob("*.py")
    if not p.name.startswith("test_") and not p.name.startswith("conftest")
)
REQUIRES_PARTITIONED = {"table_types.py"}
REQUIRES_VECTOR = {"vector_search.py"}
REQUIRES_BLOB_EXPANSION = {"blob_expansion.py"}


@pytest.mark.parametrize("script", EXAMPLE_SCRIPTS, ids=lambda p: p.name)
def test_example_script(
    script: Path,
    session: Session,
    session_kwargs: dict,
    test_bucket_name: str,
) -> None:
    if script.name in REQUIRES_PARTITIONED:
        try:
            session.features.check_partitioned()
        except errors.NotSupportedVersion:
            pytest.skip("Partitioned tables require Hydra (VAST 5.5+)")

    if script.name in REQUIRES_VECTOR:
        try:
            session.features.check_vector()
        except errors.NotSupportedVersion:
            pytest.skip("Vector tables require VAST 5.5+")
        try:
            import adbc_driver_vastdb  # noqa: F401
        except ImportError:
            if not os.environ.get("VASTDB_ADBC_DRIVER_PATH"):
                pytest.skip("ADBC driver not available for vector_search example")

    if script.name in REQUIRES_BLOB_EXPANSION:
        try:
            session.features.check_blob_expansion()
        except errors.NotSupportedVersion:
            pytest.skip("Blob expansion requires VAST 5.5+")

    env = os.environ.copy()
    env["AWS_ACCESS_KEY_ID"] = session_kwargs["access"]
    env["AWS_SECRET_ACCESS_KEY"] = session_kwargs["secret"]
    env["AWS_S3_ENDPOINT_URL"] = session_kwargs["endpoint"]
    env["VASTDB_BUCKET"] = test_bucket_name

    result = subprocess.run(
        [sys.executable, str(script)],
        env=env,
        cwd=str(EXAMPLES_DIR),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.fail(
            f"{script.name} failed with exit {result.returncode}\n"
            f"--- stdout ---\n{result.stdout}\n"
            f"--- stderr ---\n{result.stderr}"
        )
