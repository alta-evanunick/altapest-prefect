import pytest

import pandas as pd

from config import load_office_metadata, load_entity_metadata


def test_load_office_metadata_structure(monkeypatch):
    class DummyCursor:
        def fetch_pandas_all(self):
            return pd.DataFrame([{"office_id": 1, "base_url": "url", "key_lookup": "k", "token_lookup": "t"}])

    class DummyConn:
        def cursor(self):
            return DummyCursor()
        def close(self):
            pass

    monkeypatch.setattr("snowflake.connector.connect", lambda **kw: DummyConn())
    df = load_office_metadata()
    assert isinstance(df, pd.DataFrame)
    assert "office_id" in df.columns


def test_load_entity_metadata_structure(monkeypatch):
    class DummyCursor:
        def fetch_pandas_all(self):
            return pd.DataFrame([{"entity_name": "e", "endpoint": "ep", "date_fields": ["a", "b"], "is_global": False}])

    class DummyConn:
        def cursor(self):
            return DummyCursor()
        def close(self):
            pass

    monkeypatch.setattr("snowflake.connector.connect", lambda **kw: DummyConn())
    df = load_entity_metadata()
    assert isinstance(df, pd.DataFrame)
    assert "entity_name" in df.columns