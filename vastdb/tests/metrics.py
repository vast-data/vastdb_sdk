import dataclasses
import sqlite3
from typing import List

_MAP_SQLITE_TYPES = {
    str: "TEXT",
    float: "REAL",
    int: "INTEGER",
}


@dataclasses.dataclass
class Row:
    start: float
    finish: float
    table_path: str
    op: str
    nbytes: int
    rows: int
    cols: int
    pid: int
    tid: int


class Table:
    def __init__(self, conn: sqlite3.Connection, name: str):
        self.fields = dataclasses.fields(Row)
        self.conn = conn
        self.name = name
        columns = ", ".join(
            f"{f.name} {_MAP_SQLITE_TYPES[f.type]}"
            for f in self.fields
        )
        cmd = f"CREATE TABLE {self.name} ({columns})"
        self.conn.execute(cmd).fetchall()

    def insert(self, rows: List[Row]):
        args = ", ".join(["?"] * len(self.fields))
        cmd = f"INSERT INTO {self.name} VALUES ({args})"
        data = [dataclasses.astuple(row) for row in rows]
        self.conn.executemany(cmd, data).fetchall()
        self.conn.commit()
