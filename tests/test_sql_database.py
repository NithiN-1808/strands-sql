"""Tests for strands_sql.sql_database using an in-memory SQLite database."""

from __future__ import annotations

import os

import pytest

from strands_sql.models import SqlDatabaseInput
from strands_sql.sql_database import (
    _ENGINE_CACHE,
    _check_table_access,
    _get_engine,
    _is_write_query,
    _rows_to_markdown,
    _sanitize_error,
    sql_database,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SQLITE_URL = "sqlite:///:memory:"


def _tool_call(action: str, **kwargs) -> dict:
    """Build a minimal ToolUse dict."""
    return {
        "toolUseId": "test-001",
        "name": "sql_database",
        "input": {"action": action, "connection_string": SQLITE_URL, **kwargs},
    }


@pytest.fixture(autouse=True)
def clear_engine_cache():
    """Clear the engine cache before each test so tests are isolated."""
    _ENGINE_CACHE.clear()
    yield
    _ENGINE_CACHE.clear()


@pytest.fixture
def seeded_db():
    """Return a connection string for a seeded SQLite DB (file-based so data persists)."""
    import tempfile

    from sqlalchemy import create_engine, text

    db_file = tempfile.mktemp(suffix=".db")
    url = f"sqlite:///{db_file}"
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(
            text("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT
            )
        """)
        )
        conn.execute(
            text("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                amount REAL NOT NULL
            )
        """)
        )
        conn.execute(text("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')"))
        conn.execute(text("INSERT INTO users VALUES (2, 'Bob', NULL)"))
        conn.execute(text("INSERT INTO orders VALUES (1, 1, 99.99)"))
        conn.execute(text("INSERT INTO orders VALUES (2, 2, 49.50)"))
    yield url
    _ENGINE_CACHE.clear()
    engine.dispose()
    os.unlink(db_file)


# ---------------------------------------------------------------------------
# Unit tests — helpers
# ---------------------------------------------------------------------------


class TestIsWriteQuery:
    def test_select_is_not_write(self):
        assert not _is_write_query("SELECT * FROM users")

    def test_insert_is_write(self):
        assert _is_write_query("INSERT INTO users VALUES (1, 'x')")

    def test_update_is_write(self):
        assert _is_write_query("  UPDATE users SET name='y' WHERE id=1")

    def test_delete_is_write(self):
        assert _is_write_query("DELETE FROM users")

    def test_drop_is_write(self):
        assert _is_write_query("DROP TABLE users")

    def test_select_with_comment_is_not_write(self):
        assert not _is_write_query("-- insert comment\nSELECT 1")

    def test_case_insensitive(self):
        assert _is_write_query("insert into foo values (1)")


class TestSanitizeError:
    def test_hides_file_paths(self):
        exc = Exception('File "/home/secret/app.py", line 42, in run')
        result = _sanitize_error(exc)
        assert "/home/secret" not in result

    def test_truncates_long_messages(self):
        exc = Exception("x" * 1000)
        result = _sanitize_error(exc)
        assert len(result) <= 420  # 400 + "[truncated]"

    def test_short_messages_unchanged(self):
        exc = Exception("table not found")
        assert "table not found" in _sanitize_error(exc)


class TestCheckTableAccess:
    def test_allowed_tables_permits(self):
        assert _check_table_access("users", ["users", "orders"], None) is None

    def test_allowed_tables_blocks(self):
        result = _check_table_access("secrets", ["users"], None)
        assert result is not None
        assert "not in allowed_tables" in result

    def test_blocked_tables_blocks(self):
        result = _check_table_access("secrets", None, ["secrets"])
        assert result is not None
        assert "blocked" in result

    def test_no_filter_permits_all(self):
        assert _check_table_access("anything", None, None) is None

    def test_case_insensitive_allowed(self):
        assert _check_table_access("Users", ["users"], None) is None

    def test_case_insensitive_blocked(self):
        result = _check_table_access("SECRETS", None, ["secrets"])
        assert result is not None


class TestRowsToMarkdown:
    def test_empty_rows(self):
        result = _rows_to_markdown(["id", "name"], [])
        assert "no rows" in result

    def test_produces_separator(self):
        result = _rows_to_markdown(["id", "name"], [(1, "Alice")])
        assert "-" in result
        assert "Alice" in result
        assert "id" in result


# ---------------------------------------------------------------------------
# Integration tests — via the tool entry point
# ---------------------------------------------------------------------------


class TestSqlDatabaseTool:
    # --- Missing connection string ---
    def test_no_connection_string_returns_error(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        result = sql_database(
            {"toolUseId": "t1", "name": "sql_database", "input": {"action": "list_tables"}}
        )
        assert result["status"] == "error"
        assert "DATABASE_URL" in result["content"][0]["text"]

    def test_uses_env_var_connection_string(self, monkeypatch, seeded_db):
        monkeypatch.setenv("DATABASE_URL", seeded_db)
        result = sql_database(
            {"toolUseId": "t1", "name": "sql_database", "input": {"action": "list_tables"}}
        )
        assert result["status"] == "success"

    # --- list_tables ---
    def test_list_tables(self, seeded_db):
        result = sql_database(_tool_call("list_tables", connection_string=seeded_db))
        assert result["status"] == "success"
        text = result["content"][0]["text"]
        assert "users" in text
        assert "orders" in text

    def test_list_tables_respects_blocklist(self, seeded_db):
        result = sql_database(
            _tool_call("list_tables", connection_string=seeded_db, blocked_tables=["orders"])
        )
        text = result["content"][0]["text"]
        assert "orders" not in text
        assert "users" in text

    def test_list_tables_respects_allowlist(self, seeded_db):
        result = sql_database(
            _tool_call("list_tables", connection_string=seeded_db, allowed_tables=["users"])
        )
        text = result["content"][0]["text"]
        assert "orders" not in text
        assert "users" in text

    # --- describe_table ---
    def test_describe_table(self, seeded_db):
        result = sql_database(
            _tool_call("describe_table", connection_string=seeded_db, table="users")
        )
        assert result["status"] == "success"
        text = result["content"][0]["text"]
        assert "name" in text
        assert "email" in text

    def test_describe_table_missing_table_param(self, seeded_db):
        result = sql_database(
            {
                "toolUseId": "t1",
                "name": "sql_database",
                "input": {"action": "describe_table", "connection_string": seeded_db},
            }
        )
        assert result["status"] == "error"

    def test_describe_table_blocked(self, seeded_db):
        result = sql_database(
            _tool_call(
                "describe_table",
                connection_string=seeded_db,
                table="users",
                blocked_tables=["users"],
            )
        )
        assert "blocked" in result["content"][0]["text"]

    # --- schema_summary ---
    def test_schema_summary(self, seeded_db):
        result = sql_database(_tool_call("schema_summary", connection_string=seeded_db))
        assert result["status"] == "success"
        text = result["content"][0]["text"]
        assert "users" in text
        assert "orders" in text

    # --- query ---
    def test_query_select(self, seeded_db):
        result = sql_database(
            _tool_call("query", connection_string=seeded_db, sql="SELECT * FROM users")
        )
        assert result["status"] == "success"
        assert "Alice" in result["content"][0]["text"]

    def test_query_markdown_format(self, seeded_db):
        result = sql_database(
            _tool_call(
                "query",
                connection_string=seeded_db,
                sql="SELECT * FROM users",
                output_format="markdown",
            )
        )
        text = result["content"][0]["text"]
        assert "|" in text or "-" in text  # markdown table separators

    def test_query_json_format(self, seeded_db):
        import json

        result = sql_database(
            _tool_call(
                "query",
                connection_string=seeded_db,
                sql="SELECT id, name FROM users",
                output_format="json",
            )
        )
        parsed = json.loads(result["content"][0]["text"])
        assert isinstance(parsed, list)
        assert parsed[0]["name"] == "Alice"

    def test_query_respects_max_rows(self, seeded_db):
        result = sql_database(
            _tool_call("query", connection_string=seeded_db, sql="SELECT * FROM users", max_rows=1)
        )
        text = result["content"][0]["text"]
        assert "truncated" in text.lower()

    def test_query_blocked_write_in_read_only(self, seeded_db):
        result = sql_database(
            _tool_call(
                "query", connection_string=seeded_db, sql="DELETE FROM users", read_only=True
            )
        )
        assert result["status"] == "error"
        assert "read_only" in result["content"][0]["text"]

    def test_query_blocked_table(self, seeded_db):
        result = sql_database(
            _tool_call(
                "query",
                connection_string=seeded_db,
                sql="SELECT * FROM users",
                blocked_tables=["users"],
            )
        )
        assert "blocked" in result["content"][0]["text"]

    def test_query_bad_sql(self, seeded_db):
        result = sql_database(
            _tool_call(
                "query", connection_string=seeded_db, sql="SELECT * FROM nonexistent_table_xyz"
            )
        )
        assert "error" in result["content"][0]["text"].lower()

    # --- execute ---
    def test_execute_blocked_by_read_only(self, seeded_db):
        result = sql_database(
            _tool_call(
                "execute",
                connection_string=seeded_db,
                sql="INSERT INTO users VALUES (99, 'Test', NULL)",
                read_only=True,
            )
        )
        assert result["status"] == "error"
        assert "blocked" in result["content"][0]["text"]

    def test_execute_write_when_allowed(self, seeded_db):
        result = sql_database(
            _tool_call(
                "execute",
                connection_string=seeded_db,
                sql="INSERT INTO users VALUES (99, 'Test', NULL)",
                read_only=False,
            )
        )
        assert result["status"] == "success"
        assert "OK" in result["content"][0]["text"]

    def test_execute_missing_sql(self, seeded_db):
        result = sql_database(
            {
                "toolUseId": "t1",
                "name": "sql_database",
                "input": {"action": "execute", "connection_string": seeded_db, "read_only": False},
            }
        )
        assert result["status"] == "error"

    # --- engine cache ---
    def test_engine_is_cached(self, seeded_db):
        _ENGINE_CACHE.clear()
        _get_engine(seeded_db, 30)
        assert seeded_db in _ENGINE_CACHE
        engine_first = _ENGINE_CACHE[seeded_db]
        _get_engine(seeded_db, 30)
        assert _ENGINE_CACHE[seeded_db] is engine_first  # same object


# ---------------------------------------------------------------------------
# Model validation tests
# ---------------------------------------------------------------------------


class TestSqlDatabaseInput:
    def test_valid_query(self):
        m = SqlDatabaseInput(action="query", sql="SELECT 1")
        assert m.max_rows == 500
        assert m.read_only is True
        assert m.output_format == "markdown"

    def test_query_requires_sql(self):
        with pytest.raises(Exception, match="sql"):
            SqlDatabaseInput(action="query")

    def test_describe_requires_table(self):
        with pytest.raises(Exception, match="table"):
            SqlDatabaseInput(action="describe_table")

    def test_max_rows_bounds(self):
        with pytest.raises(Exception):
            SqlDatabaseInput(action="query", sql="SELECT 1", max_rows=0)
        with pytest.raises(Exception):
            SqlDatabaseInput(action="query", sql="SELECT 1", max_rows=99999)

    def test_timeout_bounds(self):
        with pytest.raises(Exception):
            SqlDatabaseInput(action="query", sql="SELECT 1", timeout=0)
