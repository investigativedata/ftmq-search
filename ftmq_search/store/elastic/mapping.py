from functools import cache
from typing import Any, Dict, List, Optional, Union

MappingProperty = Dict[str, Union[List[str], str, bool]]

ANALYSIS_SETTINGS = {
    "analysis": {
        "normalizer": {
            "osa-normalizer": {
                "type": "custom",
                "filter": ["lowercase", "asciifolding"],
            }
        },
        "analyzer": {
            "osa-analyzer": {
                "tokenizer": "standard",
                "filter": ["lowercase", "asciifolding"],
            }
        },
    },
}


def make_field(type_: str, format_: Optional[str] = None) -> MappingProperty:
    spec: MappingProperty = {"type": type_}
    if type_ == "keyword":
        spec["normalizer"] = "osa-normalizer"
    if type_ == "text":
        spec["analyzer"] = "osa-analyzer"
    if format_ is not None:
        spec["format"] = format_
    return spec


KEYWORD_FIELD = {"type": "keyword"}
# PROXY_FIELD = {"type": "object", "index": False}
PROXY_FIELD = {"type": "text"}


@cache
def make_mapping() -> Dict[str, Any]:
    properties = {
        "id": KEYWORD_FIELD,
        "schema": make_field("keyword"),
        "caption": make_field("keyword"),
        "names": make_field("keyword"),
        "datasets": KEYWORD_FIELD,
        "countries": KEYWORD_FIELD,
        "text": make_field("text"),
        "proxy": PROXY_FIELD,
    }
    return {
        "dynamic": "strict",
        "properties": properties,
        "_source": {"includes": ["proxy", "names"]},
    }
