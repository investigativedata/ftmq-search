"""
SQlite FTS5
"""

from functools import cache
from typing import Iterable

import orjson
from ftmq.query import Q
from ftmq.types import CE
from normality import normalize
from pydantic import ConfigDict
from sqlalchemy import Column, MetaData, Table, Text, Unicode, insert, select, text
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.exc import OperationalError

from ftmq_search.logging import get_logger
from ftmq_search.model import AutocompleteResult, EntityDocument, EntitySearchResult
from ftmq_search.settings import Settings
from ftmq_search.store.base import BaseStore

settings = Settings()

log = get_logger(__name__)

KEY_LEN = 255
VALUE_LEN = 65535


@cache
def get_metadata() -> MetaData:
    return MetaData()


@cache
def make_table(name: str = settings.sql_table_name) -> Table:
    metadata = get_metadata()
    return Table(
        name,
        metadata,
        Column("id", Unicode(KEY_LEN), primary_key=True, unique=True),
        Column("datasets", Unicode(VALUE_LEN), index=True),
        Column("schema", Unicode(KEY_LEN), index=True, nullable=False),
        Column("countries", Unicode(KEY_LEN), index=True, nullable=False),
        Column("caption", Unicode(VALUE_LEN), index=True, nullable=False),
        Column("names", Unicode(VALUE_LEN), index=True, nullable=False),
        Column("proxy", Text, nullable=False),
    )


@cache
def make_names_table(name: str = settings.sql_table_name) -> Table:
    metadata = get_metadata()
    return Table(
        f"{name}_names",
        metadata,
        Column("id", Unicode(KEY_LEN), nullable=False),
        Column("name", Unicode(VALUE_LEN), index=True, nullable=False),
    )


@cache
def make_fts_table(name: str = settings.sql_table_name) -> Table:
    metadata = get_metadata()
    return Table(
        f"{name}_fts",
        metadata,
        Column("id", Unicode(KEY_LEN), nullable=False),
        Column("text", Text(VALUE_LEN), nullable=False),
    )


def to_array(values: list[str]) -> str:
    if not values:
        return ""
    return f"#{'#'.join(values)}#"


def from_array(value: str) -> list[str]:
    return value.strip("#").split("#")


class SQliteStore(BaseStore):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    table_name: str = settings.sql_table_name
    table: Table
    names_table: Table
    fts_table: Table
    engine: Engine

    buffer: list[tuple[str, str, str, str, str, str, str]] = []
    fts_buffer: list[tuple[str, str]] = []
    names_buffer: list[tuple[str, str]] = []

    def __init__(self, **data):
        uri = data.get("uri", settings.uri)
        table_name = data.get("table_name", settings.sql_table_name)
        data["engine"] = create_engine(uri)
        data["table"] = make_table(table_name)
        data["names_table"] = make_names_table(table_name)
        data["fts_table"] = make_fts_table(table_name)
        super().__init__(**data)
        self.create()

    def create(self):
        metadata = get_metadata()
        metadata.create_all(
            self.engine, tables=[self.table, self.names_table], checkfirst=True
        )
        with self.engine.connect() as conn:
            try:
                conn.execute(
                    text(
                        f"CREATE VIRTUAL TABLE {self.table_name}_fts USING "
                        "fts5(id UNINDEXED, text)"
                    )
                )
            except OperationalError as e:
                if "already exists" in str(e):
                    return
                raise e

    def flush(self):
        conn = self.engine.connect()
        tx = conn.begin()
        if self.buffer:
            conn.execute(insert(self.table).values(self.buffer))
        if self.names_buffer:
            conn.execute(insert(self.names_table).values(self.names_buffer))
        if self.fts_buffer:
            conn.execute(insert(self.fts_table).values(self.fts_buffer))
        tx.commit()
        self.buffer = []
        self.fts_buffer = []
        self.names_buffer = []

    def put(self, doc: EntityDocument):
        self.buffer.append(
            (
                doc.id,
                to_array(doc.datasets),
                doc.schema_,
                to_array(doc.countries),
                doc.caption,
                to_array(doc.names),
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

    def search(self, q: str, query: Q | None = None) -> Iterable[EntitySearchResult]:
        # FIXME
        q = normalize(q, lowercase=False) or ""
        stmt = text(
            f"""SELECT s.rank, t.* FROM {self.table_name}_fts s
        LEFT JOIN {self.table_name} t ON s.id = t.id
        WHERE s.text MATCH '{q}' ORDER BY s.rank"""
        )
        with self.engine.connect() as conn:
            for res in conn.execute(stmt):
                res = dict(res._mapping)
                score = res.pop("rank")
                res["datasets"] = from_array(res["datasets"])
                res["names"] = from_array(res["names"])
                res["countries"] = from_array(res["countries"])
                res["proxy"] = orjson.loads(res["proxy"])
                yield EntitySearchResult(score=score, **res)

    def autocomplete(self, q: str) -> Iterable[AutocompleteResult]:
        q = normalize(q, lowercase=False) or ""
        stmt = select(self.names_table).where(self.names_table.c.name.ilike(f"{q}%"))
        with self.engine.connect() as conn:
            for id_, name in conn.execute(stmt):
                yield AutocompleteResult(id=id_, name=name)
