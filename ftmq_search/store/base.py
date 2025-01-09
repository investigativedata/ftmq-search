from typing import Iterable

from anystore.mixins import BaseModel
from ftmq.query import Q
from ftmq.types import CE

from ftmq_search.model import AutocompleteResult, EntityDocument, EntitySearchResult
from ftmq_search.settings import Settings

settings = Settings()


class BaseStore(BaseModel):
    uri: str = settings.uri
    index_props: list[str] = settings.index_props
    name_props: list[str] = settings.name_props

    def put(self, doc: EntityDocument) -> None:
        raise NotImplementedError

    def flush(self) -> None:
        raise NotImplementedError

    def build(self, proxies: Iterable[CE]) -> int:
        ix = 0
        for ix, proxy in enumerate(proxies, 1):
            if proxy.schema.is_a("Thing"):
                self.put(
                    EntityDocument.from_proxy(
                        proxy,
                        index_props=self.index_props,
                        name_props=self.name_props,
                    )
                )
            if ix % 10_000 == 0:
                self.flush()
        self.flush()
        return ix

    def search(self, q: str, query: Q | None = None) -> Iterable[EntitySearchResult]:
        raise NotImplementedError

    def autocomplete(self, q: str) -> Iterable[AutocompleteResult]:
        raise NotImplementedError
