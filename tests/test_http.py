import responses
import pytest

from utils.http import http_get


@responses.activate
def test_http_get_success(tmp_path):
    url = "https://example.com/api"
    responses.add(
        responses.GET,
        url,
        json={"status": "ok"},
        status=200,
    )
    result = http_get(url)
    assert result == {"status": "ok"}


@responses.activate
def test_http_get_retry(monkeypatch):
    url = "https://example.com/api"
    calls = []

    def failing_get(*args, **kwargs):
        calls.append(True)
        raise RuntimeError("fail")

    monkeypatch.setattr("requests.get", failing_get)
    with pytest.raises(RuntimeError):
        http_get(url, retries=2, backoff=0)
    assert len(calls) == 2