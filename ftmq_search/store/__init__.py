from functools import cache
from urllib.parse import urlparse

from ftmq_search.logging import get_logger
from ftmq_search.settings import Settings
from ftmq_search.store.base import BaseStore
from ftmq_search.store.sqlite import SQliteStore
from anystore.util import ensure_uri


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
    raise NotImplementedError(f"Store scheme: `{parsed.scheme}`")
