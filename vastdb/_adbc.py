import hashlib
import logging
import os
import urllib.request

from adbc_driver_manager.dbapi import Connection, Cursor, connect

log = logging.getLogger(__name__)

TXID_OVERRIDE_PROPERTY: str = "external_txid"
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
    adbc_driver_path: str, endpoint: str, access_key: str, secret_key: str, txid: int
) -> Connection:
    """Get an adbc connection in transaction."""
    return connect(
        driver=adbc_driver_path,
        db_kwargs={
            "vast.db.endpoint": endpoint,
            "vast.db.access_key": access_key,
            "vast.db.secret_key": secret_key,
        },
        # TODO re-add this after almog finishes
        # conn_kwargs={TXID_OVERRIDE_PROPERTY: str(txid)},
    )


class AdbcConnection:
    def __init__(
        self,
        adbc_driver: AdbcDriver,
        endpoint: str,
        access_key: str,
        secret_key: str,
        txid: int,
    ):
        self._adbc_conn = _get_adbc_connection(
            adbc_driver.local_path, endpoint, access_key, secret_key, txid
        )

        self._cursor = self._adbc_conn.cursor()

    @property
    def cursor(self) -> Cursor:
        return self._cursor

    def close(self):
        self._cursor.close()
