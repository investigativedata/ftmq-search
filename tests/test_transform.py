from ftmq.io import orjson, smart_stream

from ftmq_search.logic import transform
from ftmq_search.model import EntityDocument


def test_transform(fixtures_path, tmp_path, donations):
    tested = False
    for proxy in donations:
        data = EntityDocument.from_proxy(proxy)
        assert data.model_dump(by_alias=True) == {
            "id": "6d03aec76fdeec8f9697d8b19954ab6fc2568bc8",
            "caption": "MLPD",
            "schema": "Organization",
            "datasets": ["donations"],
            "countries": [],
            "names": ["MLPD"],
            "text": "MLPD",
        }

        tested = True
        break
    assert tested

    # use the worker
    out = tmp_path / "transformed.json"
    transform(fixtures_path / "donations.ijson", out)
    transformed = [d for d in smart_stream(out)]
    assert len(transformed) == 184
    data = orjson.loads(transformed[0])
    assert "donations" in EntityDocument(**data).datasets
