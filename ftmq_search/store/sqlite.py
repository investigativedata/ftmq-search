"""
SQlite FTS5 
"""

from normality import normalize
import orjson
import sqlite3
from typing import Iterable
from ftmq.types import CE
from functools import cache

from pydantic import ConfigDict

from ftmq_search.logging import get_logger
from ftmq_search.model import AutocompleteResult, EntityDocument, EntitySearchResult
from ftmq_search.store.base import BaseStore
from ftmq_search.settings import Settings

settings = Settings()

log = get_logger(__name__)


def dict_factory(cursor: sqlite3.Cursor, row: sqlite3.Row) -> dict[str, str]:
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


@cache
def get_connection(uri: str = settings.uri) -> sqlite3.Connection:
    uri = uri.replace("sqlite:///", "")
    con = sqlite3.connect(uri, uri=True)
    con.row_factory = dict_factory
    return con


class SQliteStore(BaseStore):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    table_name: str = "ftmqs"
    connection: sqlite3.Connection
    buffer: list[tuple[str, str, str, str, str, str, str]] = []
    fts_buffer: list[tuple[str, str]] = []
    names_buffer: list[tuple[str, str]] = []

    def __init__(self, **data):
        uri = data.get("uri")
        data["connection"] = get_connection(uri)
        super().__init__(**data)
        self.create()

    def create(self):
        try:
            self.connection.execute(
                f"CREATE TABLE {self.table_name} (id TEXT, datasets JSON, schema TEXT, countries JSON, caption TEXT, names JSON, proxy JSON)"
            )
            self.connection.execute(
                f"CREATE INDEX {self.table_name}__ix ON {self.table_name}(id)"
            )
            self.connection.execute(
                f"CREATE INDEX {self.table_name}__sx ON {self.table_name}(schema)"
            )
            self.connection.execute(
                f"CREATE INDEX {self.table_name}__cx ON {self.table_name}(caption)"
            )
            self.connection.execute("PRAGMA mmap_size = 30000000000")
            self.connection.execute(
                f"CREATE VIRTUAL TABLE {self.table_name}__fts USING fts5(id UNINDEXED, text)"
            )
            self.connection.execute(
                f"CREATE TABLE {self.table_name}_names (id TEXT, name TEXT)"
            )
            self.connection.execute(
                f"CREATE INDEX {self.table_name}_names__nx ON {self.table_name}_names(name)"
            )
            self.connection.commit()
        except sqlite3.OperationalError as e:
            if f"{self.table_name} already exists" in str(e):
                return
            raise e

    def drop(self):
        self.connection.execute(f"DELETE TABLE IF EXISTS {self.table_name}")
        self.connection.execute(f"DELETE TABLE IF EXISTS {self.table_name}__fts")
        self.create()

    def flush(self):
        if self.buffer:
            self.connection.executemany(
                f"INSERT INTO {self.table_name} VALUES (?, ?, ?, ?, ?, ?, ?)",
                self.buffer,
            )
            self.connection.commit()
        if self.fts_buffer:
            self.connection.executemany(
                f"INSERT INTO {self.table_name}__fts VALUES (?, ?)", self.fts_buffer
            )
            self.connection.commit()
        if self.names_buffer:
            self.connection.executemany(
                f"INSERT INTO {self.table_name}_names VALUES (?, ?)", self.names_buffer
            )
            self.connection.commit()
        self.buffer = []
        self.fts_buffer = []
        self.names_buffer = []

    def put(self, doc: EntityDocument):
        self.buffer.append(
            (
                doc.id,
                orjson.dumps(doc.datasets).decode(),
                doc.schema_,
                orjson.dumps(doc.countries).decode(),
                doc.caption,
                orjson.dumps(doc.names).decode(),
                doc.proxy.model_dump_json(),
            )
        )
        self.fts_buffer.append((doc.id, doc.text))
        for name in doc.names:
            self.names_buffer.append((doc.id, name))
        if len(self.buffer) == 10_000:
            log.info("Indexing 10000 proxies", uri=self.uri)
            self.flush()

    def build(self, proxies: Iterable[CE]) -> int:
        res = super().build(proxies)
        log.info(f"Indexing {len(self.buffer)} proxies", uri=self.uri)
        self.flush()
        return res

    def search(self, q: str) -> Iterable[EntitySearchResult]:
        q = normalize(q, lowercase=False) or ""
        stmt = f"""SELECT s.rank, t.* FROM {self.table_name}__fts s
        LEFT JOIN {self.table_name} t ON s.id = t.id
        WHERE s.text MATCH ? ORDER BY s.rank"""
        for res in self.connection.execute(stmt, (q,)):
            score = res.pop("rank")
            res["datasets"] = orjson.loads(res["datasets"])
            res["names"] = orjson.loads(res["names"])
            res["countries"] = orjson.loads(res["countries"])
            res["proxy"] = orjson.loads(res["proxy"])
            yield EntitySearchResult(score=score, **res)

    def autocomplete(self, q: str) -> Iterable[AutocompleteResult]:
        q = normalize(q, lowercase=False) or ""
        stmt = f"""SELECT * FROM {self.table_name}_names WHERE name LIKE '{q}%' 
        ORDER BY length(name)"""
        for res in self.connection.execute(stmt):
            yield AutocompleteResult(**res)
