import hashlib
import logging
import os
import urllib.request
from typing import Optional

import pyarrow as pa
import sqlglot
from adbc_driver_manager.dbapi import Connection, Cursor, connect
from sqlglot import exp

from vastdb._internal import VectorIndex
from vastdb._table_interface import IbisPredicate
from vastdb.table_metadata import TableRef

log = logging.getLogger(__name__)


TXID_OVERRIDE_PROPERTY: str = "vast.db.external_txid"
END_USER_PROPERTY: str = "vast.db.end_user"
VAST_DIST_ALIAS = "vast_pysdk_vector_dist"
DEFAULT_ADBC_DRIVER_CACHE_DIR: str = "~/.vast/adbc_drivers_cache"
DEFAULT_ADBC_DRIVER_CACHE_BY_URL_DIR: str = f"{DEFAULT_ADBC_DRIVER_CACHE_DIR}/by_url"


class LocalAdbcDriverNotFound(Exception):
    """LocalAdbcDriverNotFound."""

    pass


class RemoteAdbcDriverDownloadFailed(Exception):
    """RemoteAdbcDriverDownloadFailed."""

    pass


class AdbcDriver:
    _local_path: str

    def __init__(self, local_path: str):
        self._local_path = local_path

    @staticmethod
    def from_local_path(local_path: str) -> "AdbcDriver":
        """AdbcDriver from a local_path to shared-library."""
        if not os.path.exists(local_path):
            raise LocalAdbcDriverNotFound(local_path)

        return AdbcDriver(local_path)

    @staticmethod
    def from_url(url: str) -> "AdbcDriver":
        """AdbcDriver to be downloaded by url to shared-library (uses cache if exists)."""
        expected_local_path = AdbcDriver._url_to_local_path(url)

        if os.path.exists(expected_local_path):
            return AdbcDriver(expected_local_path)

        AdbcDriver._download_driver(url, expected_local_path)
        return AdbcDriver(expected_local_path)

    @staticmethod
    def _url_to_local_path(url: str) -> str:
        url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
        return os.path.join(DEFAULT_ADBC_DRIVER_CACHE_BY_URL_DIR, url_hash)

    @staticmethod
    def _download_driver(url: str, target_path: str):
        os.makedirs(os.path.dirname(target_path), exist_ok=True)

        try:
            log.info(f"Downloading ADBC driver from {url} to {target_path}...")
            urllib.request.urlretrieve(url, target_path)
            log.info(f"Successfully downloaded driver to {target_path}.")
        except Exception as e:
            raise RemoteAdbcDriverDownloadFailed(
                f"Failed to download ADBC driver from {url}: {e}"
            ) from e

    @property
    def local_path(self) -> str:
        return self._local_path


def _get_adbc_connection(
    adbc_driver_path: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
    txid: int,
    end_user: Optional[str],
) -> Connection:
    """Get an adbc connection in transaction."""
    conn_kwargs = {TXID_OVERRIDE_PROPERTY: str(txid)}
    if end_user is not None:
        conn_kwargs[END_USER_PROPERTY] = end_user

    return connect(
        driver=adbc_driver_path,
        db_kwargs={
            "vast.db.endpoint": endpoint,
            "vast.db.access_key": access_key,
            "vast.db.secret_key": secret_key,
        },
        conn_kwargs=conn_kwargs,
    )


def _remove_table_qualification_from_columns(expression: exp.Expression):
    """Goes over all columns which are fully qualified with "t0" table reference (ibis default table qualification for unbound tables.

    Note: use only if one table is involved - if two tables exist in the expression columns might become ambiguous.
    """
    for col in expression.find_all(exp.Column):
        col.set("table", None)
    return expression


def _ibis_to_qe_predicates(predicate: IbisPredicate) -> str:
    ibis_sql = predicate.to_sql()
    parsed = sqlglot.parse_one(ibis_sql)

    # currently there is a single table
    # removing the
    without_table_qualification = _remove_table_qualification_from_columns(
        parsed.expressions[0].this
    )

    return without_table_qualification.sql()


def _vector_search_sql(
    query_vector: list[float],
    vector_index: VectorIndex,
    table_ref: TableRef,
    columns: list[str],
    limit: int,
    predicate: Optional[IbisPredicate] = None,
) -> str:
    query_vector_dim = len(query_vector)

    query_vector_literal = f"{query_vector}::FLOAT[{query_vector_dim}]"
    dist_func = f"{vector_index.sql_distance_function}({vector_index.column}::FLOAT[{query_vector_dim}], {query_vector_literal})"
    dist_alias = f"{dist_func} as {VAST_DIST_ALIAS}"

    projection_str = ",".join(columns + [dist_alias])

    if predicate is not None:
        where = f"WHERE {_ibis_to_qe_predicates(predicate)}"
    else:
        where = ""

    return f"""
            SELECT {projection_str}
            FROM {table_ref.query_engine_full_path}
            {where}
            ORDER BY {VAST_DIST_ALIAS}
            LIMIT {limit}"""


class AdbcConnection:
    def __init__(
        self,
        adbc_driver: AdbcDriver,
        endpoint: str,
        access_key: str,
        secret_key: str,
        txid: int,
        end_user: Optional[str] = None,
    ):
        self._adbc_conn = _get_adbc_connection(
            adbc_driver.local_path, endpoint, access_key, secret_key, txid, end_user
        )

        self._cursor = self._adbc_conn.cursor()

    @property
    def cursor(self) -> Cursor:
        return self._cursor

    def close(self):
        self._cursor.close()

    def vector_search(
        self,
        query_vector: list[float],
        vector_index: VectorIndex,
        table_ref: TableRef,
        columns: list[str],
        limit: int,
        predicate: Optional[IbisPredicate] = None,
    ) -> pa.RecordBatchReader:
        """Top-n on vector-column."""
        sql = _vector_search_sql(
            query_vector=query_vector,
            vector_index=vector_index,
            table_ref=table_ref,
            columns=columns,
            limit=limit,
            predicate=predicate,
        )

        self._cursor.execute(sql)
        return self._cursor.fetch_record_batch()
