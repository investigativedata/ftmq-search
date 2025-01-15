from functools import cache
from urllib.parse import urlparse

from anystore.util import SCHEME_MEMORY, ensure_uri

from ftmq_search.logging import get_logger
from ftmq_search.settings import Settings
from ftmq_search.store.base import BaseStore
from ftmq_search.store.elastic.store import ElasticStore
from ftmq_search.store.sqlite import SQliteStore
from ftmq_search.store.tantivy import TantivyStore

log = get_logger(__name__)


@cache
def get_store(**kwargs) -> BaseStore:
    settings = Settings()
    uri = kwargs.pop("uri", None)
    if uri is None:
        if settings.yaml_uri is not None:
            store = BaseStore.from_yaml_uri(settings.yaml_uri, **kwargs)
            return get_store(**store.model_dump())
        if settings.json_uri is not None:
            store = BaseStore.from_json_uri(settings.json_uri, **kwargs)
            return get_store(**store.model_dump())
        uri = settings.uri
    uri = ensure_uri(uri)
    parsed = urlparse(uri)
    if parsed.scheme == "sqlite":
        return SQliteStore(uri=uri, **kwargs)
    if parsed.scheme in ("tantivy", SCHEME_MEMORY):
        return TantivyStore(uri=uri, memory=parsed.scheme == SCHEME_MEMORY)
    if parsed.scheme in ("http", "https"):
        return ElasticStore(uri=uri, **kwargs)
    raise NotImplementedError(f"Store scheme: `{parsed.scheme}`")
