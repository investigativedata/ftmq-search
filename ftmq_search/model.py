from typing import Any, Iterable, Self

from banal import ensure_list
from followthemoney.types import registry
from followthemoney.util import join_text
from ftmq.model import Entity
from ftmq.types import CE
from pydantic import BaseModel, ConfigDict, Field

from ftmq_search.exceptions import IntegrityError
from ftmq_search.settings import Settings

settings = Settings()


def get_names_values(proxy: CE, props: list[str] = settings.name_props) -> list[str]:
    if not props:
        return proxy.get_type_values(registry.name)
    names = []
    for prop in props:
        names.extend(proxy.get(prop, quiet=True))
    return names


def get_index_values(proxy: CE, props: list[str] = settings.index_props) -> list[str]:
    if not props:
        props = proxy.schema.featured
    values = []
    for prop in props:
        values.extend(proxy.get(prop, quiet=True))
    values.extend(proxy.get_type_values(registry.identifier))
    return values


class EntityDocument(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(..., examples=["NK-A7z...."])
    caption: str = Field(..., examples=["Jane Doe"])
    schema_: str = Field(..., examples=["LegalEntity"], alias="schema")
    datasets: list[str] = Field([], examples=[["us_ofac_sdn"]])
    countries: list[str] = Field([], examples=[["de"]])
    names: list[str]
    text: str = ""

    @classmethod
    def from_proxy(
        cls,
        proxy: CE,
        index_props: list[str] = settings.index_props,
        name_props: list[str] = settings.name_props,
    ) -> Self:
        if proxy.id is None:
            raise IntegrityError("Entity has no ID!")
        names = get_names_values(proxy, name_props)
        index = get_index_values(proxy, index_props)
        texts = set(names + index)
        text = join_text(*texts) or ""
        return cls(
            id=proxy.id,
            datasets=list(proxy.datasets),
            schema=proxy.schema.name,
            countries=proxy.countries,
            caption=proxy.caption,
            names=names,
            text=text,
        )


class EntitySearchResult(BaseModel):
    id: str = Field(..., examples=["NK-A7z...."])
    proxy: Entity
    score: float = 1

    def __init__(self, /, **data: Any) -> None:
        if "proxy" not in data:
            data["proxy"] = self.make_entity(**data)
        super().__init__(**data)

    @staticmethod
    def make_entity(
        id: str,
        schema: str,
        datasets: Iterable[str],
        caption: str,
        names: Iterable[str],
        countries: Iterable[str] | None = None,
        **kwargs: Any,
    ) -> Entity:
        return Entity(
            id=id,
            schema=schema,
            datasets=list(datasets),
            caption=caption,
            properties={"name": list(names), "country": ensure_list(countries)},
            referents=[],
        )


class AutocompleteResult(BaseModel):
    id: str
    name: str
