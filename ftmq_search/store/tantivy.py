"""
Tantivy store
"""

import os
from functools import cache
from typing import Iterable

import tantivy
from anystore.util import model_dump
from ftmq.query import Q
from normality import normalize
from pydantic import ConfigDict

from ftmq_search.model import AutocompleteResult, EntityDocument, EntitySearchResult
from ftmq_search.store.base import BaseStore


def or_(key: str, items: Iterable[str]) -> str:
    q = " OR ".join(f"{key}:{i}" for i in items)
    return f"({q})"


@cache
def make_schema() -> tantivy.Schema:
    schema_builder = tantivy.SchemaBuilder()
    schema_builder.add_text_field("id", tokenizer_name="raw", stored=True)
    schema_builder.add_text_field("datasets", tokenizer_name="raw", stored=True)
    schema_builder.add_text_field("schema", tokenizer_name="raw", stored=True)
    schema_builder.add_text_field("countries", tokenizer_name="raw", stored=True)
    schema_builder.add_text_field("caption", stored=True)
    schema_builder.add_text_field("names", stored=True)
    schema_builder.add_text_field("text", stored=False)
    return schema_builder.build()


class TantivyStore(BaseStore):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    memory: bool = False
    index: tantivy.Index
    buffer: list[tantivy.Document] = []

    def __init__(self, **data):
        schema = make_schema()
        if data.get("memory"):
            data["index"] = tantivy.Index(schema)
        else:
            uri = data["uri"][len("tantivy://") :]
            os.makedirs(uri, exist_ok=True)
            data["index"] = tantivy.Index(schema, uri)
        super().__init__(**data)

    def put(self, doc: EntityDocument) -> None:
        self.buffer.append(tantivy.Document(**model_dump(doc)))
        if len(self.buffer) == 100_000:
            self.flush()

    def flush(self) -> None:
        writer = self.index.writer()
        for doc in self.buffer:
            writer.add_document(doc)
        writer.commit()
        self.buffer = []

    def search(self, q: str, query: Q | None = None) -> Iterable[EntitySearchResult]:
        searcher = self.get_searcher()
        t_query = self.parse_query(q, query)
        res = searcher.search(t_query)
        for score, doc_address in res.hits:
            doc = searcher.doc(doc_address)
            data = doc.to_dict()
            data["id"] = doc.get_first("id")
            data["caption"] = doc.get_first("caption")
            data["schema"] = doc.get_first("schema")
            yield EntitySearchResult(**data, score=score)

    def autocomplete(self, q: str) -> Iterable[AutocompleteResult]:
        nq = normalize(q)
        searcher = self.get_searcher()
        t_query = self.index.parse_query(f"{q}*", ["names"])
        res = searcher.search(t_query)
        for _, doc_address in res.hits:
            doc = searcher.doc(doc_address)
            for name in doc.get_all("names"):
                if normalize(name).startswith(nq):
                    yield AutocompleteResult(id=doc.get_first("id"), name=name)

    def get_searcher(self) -> tantivy.Searcher:
        self.index.reload()
        return self.index.searcher()

    def parse_query(self, q: str, query: Q) -> tantivy.Query:
        stmt = q
        if query is not None:
            if query.dataset_names:
                stmt += f" AND {or_("datasets", query.dataset_names)}"
            if query.schemata_names:
                stmt += f" AND {or_("schema", query.schemata_names)}"
            if query.countries:
                stmt += f" AND {or_("countries", query.countries)}"
        return self.index.parse_query(stmt)
