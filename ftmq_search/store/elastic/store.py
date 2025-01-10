"""
Elasticsearch (and hopefully OpenSearch)
"""

from functools import cache
from typing import Any, Iterable

from elasticsearch import ApiError, Elasticsearch
from elasticsearch.helpers import BulkIndexError, bulk
from ftmq.query import Q
from normality import normalize
from pydantic import ConfigDict

from ftmq_search.exceptions import ElasticError
from ftmq_search.logging import get_logger
from ftmq_search.model import AutocompleteResult, EntityDocument, EntitySearchResult
from ftmq_search.settings import DEBUG, ElasticSettings, Settings
from ftmq_search.store.base import BaseStore
from ftmq_search.store.elastic.mapping import ANALYSIS_SETTINGS, make_mapping
from ftmq_search.store.elastic.query import build_autocomplete_query, build_query

base_settings = Settings()
settings = ElasticSettings()

log = get_logger(__name__)


@cache
def create_engine(
    uri: str, user: str | None = None, password: str | None = None
) -> Elasticsearch:
    basic_auth = None
    if user and password:
        basic_auth = (user, password)
    return Elasticsearch(hosts=[uri], basic_auth=basic_auth)


class ElasticStore(BaseStore):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    buffer: list[dict[str, Any]] = []
    index: str = settings.index
    engine: Elasticsearch

    def __init__(self, **data):
        uri = data.get("uri", base_settings.uri)
        data["engine"] = create_engine(uri, settings.user, settings.password)
        super().__init__(**data)
        # self.init()

    def flush(self):
        log.info(
            f"Indexing {len(self.buffer)} proxies ...", uri=self.uri, index=self.index
        )
        try:
            bulk(self.engine, self.buffer, stats_only=not DEBUG)
        except BulkIndexError as e:
            if DEBUG:
                raise e
            log.error(f"Indexing error: `{e}`", uri=self.uri, index=self.index)
        self.buffer = []

    def put(self, doc: EntityDocument):
        source = doc.model_dump(by_alias=True)
        self.buffer.append({"_id": doc.id, "_index": self.index, "_source": source})
        if len(self.buffer) == 10_000:
            self.flush()

    def search(self, q: str, query: Q | None = None) -> Iterable[EntitySearchResult]:
        res = self.engine.search(query=build_query(q, query), index=self.index)
        for hit in res["hits"]["hits"]:
            yield EntitySearchResult(
                id=hit["_id"], score=hit["_score"], **hit["_source"]
            )

    def autocomplete(self, q: str) -> Iterable[AutocompleteResult]:
        res = self.engine.search(query=build_autocomplete_query(q), index=self.index)
        nq = normalize(q)
        for hit in res["hits"]["hits"]:
            for name in hit["_source"]["names"]:
                if normalize(name).startswith(nq):
                    yield AutocompleteResult(id=hit["_id"], name=name)

    def init(self):
        try:
            self.engine.indices.create(
                index=self.index, mappings=make_mapping(), settings=ANALYSIS_SETTINGS
            )
            log.info("Create index", uri=self.uri, index=self.index)
        except ApiError as exc:
            if exc.error == "resource_already_exists_exception":
                log.debug("Index already exists.", uri=self.uri, index=self.index)
                return
            raise ElasticError(f"Could not create index: {exc}") from exc

    def make_logstash(self) -> str:
        return (
            'input { stdin { } } output { elasticsearch { hosts => ["%s"] index => "%s" } }'
            % (self.uri, self.index)
        )
