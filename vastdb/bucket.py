from . import errors, schema, transaction


from dataclasses import dataclass
import logging

log = logging.getLogger(__name__)


@dataclass
class Snapshot:
    name: str
    bucket: "Bucket"


@dataclass
class Bucket:
    name: str
    tx: "transaction.Transaction"

    def create_schema(self, path: str) -> "schema.Schema":
        self.tx._rpc.api.create_schema(self.name, path, txid=self.tx.txid)
        log.info("Created schema: %s", path)
        return self.schema(path)

    def schema(self, path: str) -> "schema.Schema":
        s = self.schemas(path)
        log.debug("schema: %s", s)
        if not s:
            raise errors.NotFoundError(f"Schema '{path}' was not found in bucket: {self.name}")
        assert len(s) == 1, f"Expected to receive only a single schema, but got: {len(s)}. ({s})"
        log.debug("Found schema: %s", s[0].name)
        return s[0]

    def schemas(self, name: str = None) -> ["schema.Schema"]:
        schemas = []
        next_key = 0
        exact_match = bool(name)
        log.debug("list schemas param: schema=%s, exact_match=%s", name, exact_match)
        while True:
            bucket_name, curr_schemas, next_key, is_truncated, _ = \
                self.tx._rpc.api.list_schemas(bucket=self.name, next_key=next_key, txid=self.tx.txid,
                                               name_prefix=name, exact_match=exact_match)
            if not curr_schemas:
                break
            schemas.extend(curr_schemas)
            if not is_truncated:
                break

        return [schema.Schema(name=name, bucket=self) for name, *_ in schemas]

    def snapshots(self) -> [Snapshot]:
        snapshots = []
        next_key = 0
        while True:
            curr_snapshots, is_truncated, next_key = \
                self.tx._rpc.api.list_snapshots(bucket=self.name, next_token=next_key)
            if not curr_snapshots:
                break
            snapshots.extend(curr_snapshots)
            if not is_truncated:
                break

        return [Snapshot(name=snapshot, bucket=self) for snapshot in snapshots]
