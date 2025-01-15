from typing import Iterable

from anystore.mixins import BaseModel
from ftmq.query import Q

from ftmq_search.model import AutocompleteResult, EntityDocument, EntitySearchResult
from ftmq_search.settings import Settings

settings = Settings()


class BaseStore(BaseModel):
    uri: str = settings.uri

    def put(self, doc: EntityDocument) -> None:
        raise NotImplementedError

    def flush(self) -> None:
        raise NotImplementedError

    def search(self, q: str, query: Q | None = None) -> Iterable[EntitySearchResult]:
        raise NotImplementedError

    def autocomplete(self, q: str) -> Iterable[AutocompleteResult]:
        raise NotImplementedError
