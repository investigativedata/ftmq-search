from pathlib import Path

import pytest
from ftmq.io import smart_read_proxies

FIXTURES_PATH = (Path(__file__).parent / "fixtures").absolute()


@pytest.fixture(scope="module")
def fixtures_path():
    return FIXTURES_PATH


@pytest.fixture(scope="module")
def donations():
    return smart_read_proxies(FIXTURES_PATH / "donations.ijson")
