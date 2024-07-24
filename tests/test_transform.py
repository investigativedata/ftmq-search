from ftmq_search.model import EntityDocument


def test_transform(donations):
    tested = False
    for proxy in donations:
        data = EntityDocument.from_proxy(proxy)
        assert data.model_dump(by_alias=True) == {
            "id": "6d03aec76fdeec8f9697d8b19954ab6fc2568bc8",
            "caption": "MLPD",
            "schema": "Organization",
            "datasets": ["donations"],
            "names": ["MLPD"],
            "text": "MLPD",
            "proxy": {
                "id": "6d03aec76fdeec8f9697d8b19954ab6fc2568bc8",
                "caption": "MLPD",
                "schema": "Organization",
                "properties": {"name": ["MLPD"]},
                "datasets": ["donations"],
                "referents": [],
            },
        }

        tested = True
        break
    assert tested
