from typing import Any

from ftmq.query import Query


def build_query(q: str, query: Query | None) -> dict[str, Any]:
    query = query or Query()
    filters = []
    if query.dataset_names:
        filters.append({"terms": {"datasets": list(query.dataset_names)}})
    if query.schemata_names:
        filters.append({"terms": {"schema": list(query.schemata_names)}})
    if query.countries:
        filters.append({"terms": {"countries": list(query.countries)}})
    return {
        "bool": {
            "filter": filters,
            "must": {"query_string": {"default_field": "text", "query": q}},
        }
    }


def build_autocomplete_query(q: str) -> dict[str, Any]:
    return {"prefix": {"names": q}}
