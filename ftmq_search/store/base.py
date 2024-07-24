from typing import Iterable

from anystore.mixins import BaseModel
from ftmq.types import CE

from ftmq_search.logging import get_logger
from ftmq_search.model import AutocompleteResult, EntityDocument, EntitySearchResult
from ftmq_search.settings import Settings

settings = Settings()

log = get_logger(__name__)


class BaseStore(BaseModel):
    uri: str = settings.uri
    display_props: list[str] = settings.display_props
    index_props: list[str] = settings.index_props
    name_props: list[str] = settings.name_props

    def put(self, doc: EntityDocument) -> None:
        raise NotImplementedError

    def build(self, proxies: Iterable[CE]) -> int:
        ix = 0
        for ix, proxy in enumerate(proxies, 1):
            if proxy.schema.is_a("Thing"):
                self.put(
                    EntityDocument.from_proxy(
                        proxy,
                        display_props=self.display_props,
                        index_props=self.index_props,
                        name_props=self.name_props,
                    )
                )
            if ix % 10_000 == 0:
                log.info(f"Loading proxy `{ix}` ...", uri=self.uri)
        return ix

    def search(self, q: str) -> Iterable[EntitySearchResult]:
        raise NotImplementedError

    def autocomplete(self, q: str) -> Iterable[AutocompleteResult]:
        raise NotImplementedError
