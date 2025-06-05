import pandas as pd
import pytest

from utils.snowflake import bulk_insert_json


def test_bulk_insert_json_no_rows(monkeypatch):
    class DummyCursor:
        def execute(self, sql, *args):
            assert "INSERT INTO" in sql
        def close(self):
            pass
    class DummyConn:
        def cursor(self):
            return DummyCursor()
        def close(self):
            pass

    monkeypatch.setattr("snowflake.connector.connect", lambda **kw: DummyConn())
    # should not raise
    bulk_insert_json("RAW.test_table", [])