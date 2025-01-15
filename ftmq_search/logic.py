from typing import Iterable

import orjson
from anystore.io import DEFAULT_WRITE_MODE, Uri, logged_items, smart_open, smart_stream
from ftmq.io import smart_stream_proxies
from ftmq.types import CE, SE

from ftmq_search.model import ALLTHETHINGS, EntityDocument
from ftmq_search.store.base import BaseStore


def transform(in_uri: Uri, out_uri: Uri) -> None:
    with smart_open(out_uri, DEFAULT_WRITE_MODE) as o:
        for proxy in logged_items(
            smart_stream_proxies(in_uri),
            "Transform",
            uri=in_uri,
            item_name="Entity",
        ):
            if ALLTHETHINGS.apply(proxy):
                doc = EntityDocument.from_proxy(proxy)
                data = doc.model_dump(by_alias=True, mode="json")
                line = orjson.dumps(data, option=orjson.OPT_APPEND_NEWLINE)
                o.write(line)


def index(in_uri: Uri, store: BaseStore) -> None:
    for line in logged_items(
        smart_stream(in_uri),
        "Index",
        from_uri=in_uri,
        uri=store.uri,
        item_name="EntityDocument",
    ):
        doc = EntityDocument(**orjson.loads(line))
        store.put(doc)
    store.flush()


def index_proxies(proxies: Iterable[CE | SE], store: BaseStore) -> None:
    proxies = ALLTHETHINGS.apply_iter(proxies)
    for proxy in logged_items(proxies, "Index", item_name="Proxy", uri=store.uri):
        doc = EntityDocument.from_proxy(proxy)
        store.put(doc)
    store.flush()
