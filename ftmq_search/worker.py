from typing import TYPE_CHECKING, Any, Iterable

from anystore.worker import Worker, WorkerStatus, Writer, WriteWorker
from ftmq.io import smart_read_proxies
from nomenklatura.entity import CE

from ftmq_search.model import ALLTHETHINGS, EntityDocument

if TYPE_CHECKING:
    from ftmq_search.store.base import BaseStore


class TransformWorker(WriteWorker):
    """
    Parallelize transforming
    """

    def handle_task(self, task: Any) -> Any:
        data = EntityDocument.from_proxy(task)
        content = data.model_dump_json(by_alias=True).encode()
        self.write(content + b"\n")


def transform(in_uri: str, out_uri: str) -> WorkerStatus:
    writer = Writer(out_uri)
    tasks = smart_read_proxies(in_uri, query=ALLTHETHINGS)
    worker = TransformWorker(tasks=tasks, writer=writer)
    return worker.run()


class IndexWorker(Worker):
    """
    Parallelize indexing
    """

    def __init__(self, store: "BaseStore", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.store = store

    def handle_task(self, task: Any) -> Any:
        doc = EntityDocument.from_proxy(task)
        self.store.put(doc)

    def done(self) -> None:
        self.store.flush()


def index(proxies: Iterable[CE], store: "BaseStore") -> WorkerStatus:
    proxies = ALLTHETHINGS.apply_iter(proxies)
    worker = IndexWorker(store, tasks=proxies)
    return worker.run()
