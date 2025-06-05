import pytest

from utils.pagination import chunk_list


def test_chunk_list_exact():
    data = list(range(6))
    chunks = list(chunk_list(data, 3))
    assert chunks == [[0, 1, 2], [3, 4, 5]]


def test_chunk_list_remainder():
    data = list(range(5))
    chunks = list(chunk_list(data, 2))
    assert chunks == [[0, 1], [2, 3], [4]]