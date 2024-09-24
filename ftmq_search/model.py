from typing import Self

from followthemoney.types import registry
from followthemoney.util import join_text
from ftmq.model import Entity
from ftmq.types import CE
from ftmq.util import get_dehydrated_proxy, make_proxy
from pydantic import BaseModel, ConfigDict, Field

from ftmq_search.exceptions import IntegrityError
from ftmq_search.settings import Settings

settings = Settings()


def get_display_proxy(
    proxy: CE, display_props: list[str] = settings.display_props
) -> Entity:
    dehydrated = get_dehydrated_proxy(proxy)
    for prop in display_props:
        dehydrated.add(prop, proxy.get(prop, quiet=True), quiet=True, cleaned=True)
    return Entity.from_proxy(dehydrated)


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
    proxy: Entity

    def as_proxy(self) -> CE:
        return make_proxy(self.proxy.model_dump(by_alias=True))

    @classmethod
    def from_proxy(
        cls,
        proxy: CE,
        display_props: list[str] = settings.display_props,
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
            proxy=get_display_proxy(proxy, display_props),
        )


class EntitySearchResult(BaseModel):
    id: str = Field(..., examples=["NK-A7z...."])
    proxy: Entity
    score: float = 1


class AutocompleteResult(BaseModel):
    id: str
    name: str
