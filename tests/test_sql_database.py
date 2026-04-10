"""Tests for strands_sql — covers StrandsSQL class, sql_database tool handler,
helper functions, and model validation with full edge-case coverage."""

from __future__ import annotations

import json
import os
import tempfile

import pytest
from sqlalchemy import create_engine, text

from strands_sql.models import SqlDatabaseInput
from strands_sql.sql_database import (
    _ENGINE_CACHE,
    _check_table_access,
    _get_engine,
    _is_write_query,
    _rows_to_markdown,
    _sanitize_error,
    sql_database,
    StrandsSQL,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

SQLITE_MEMORY = "sqlite:///:memory:"


def _tool_call(action: str, **kwargs) -> dict:
    """Build a minimal ToolUse dict wired to the seeded DB."""
    return {
        "toolUseId": "test-001",
        "name": "sql_database",
        "input": {"action": action, **kwargs},
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clear_engine_cache():
    _ENGINE_CACHE.clear()
    yield
    _ENGINE_CACHE.clear()


@pytest.fixture
def db_url(tmp_path):
    """Seeded file-based SQLite DB; yields its SQLAlchemy URL."""
    db_file = tmp_path / "test.db"
    url = f"sqlite:///{db_file}"
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                amount REAL NOT NULL
            )
        """))
        conn.execute(text("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')"))
        conn.execute(text("INSERT INTO users VALUES (2, 'Bob', NULL)"))
        conn.execute(text("INSERT INTO orders VALUES (1, 1, 99.99)"))
        conn.execute(text("INSERT INTO orders VALUES (2, 2, 49.50)"))
    yield url
    engine.dispose()


# ===========================================================================
# Unit tests — pure helper functions
# ===========================================================================


class TestIsWriteQuery:
    def test_select_is_not_write(self):
        assert not _is_write_query("SELECT * FROM users")

    def test_select_with_leading_whitespace(self):
        assert not _is_write_query("   SELECT 1")

    def test_insert_is_write(self):
        assert _is_write_query("INSERT INTO users VALUES (1, 'x')")

    def test_update_is_write(self):
        assert _is_write_query("  UPDATE users SET name='y' WHERE id=1")

    def test_delete_is_write(self):
        assert _is_write_query("DELETE FROM users")

    def test_drop_is_write(self):
        assert _is_write_query("DROP TABLE users")

    def test_create_is_write(self):
        assert _is_write_query("CREATE TABLE foo (id INTEGER)")

    def test_alter_is_write(self):
        assert _is_write_query("ALTER TABLE users ADD COLUMN age INTEGER")

    def test_truncate_is_write(self):
        assert _is_write_query("TRUNCATE TABLE users")

    def test_replace_is_write(self):
        assert _is_write_query("REPLACE INTO users VALUES (1, 'x')")

    def test_merge_is_write(self):
        assert _is_write_query("MERGE INTO users USING src ON users.id = src.id")

    def test_exec_is_write(self):
        assert _is_write_query("EXEC sp_something")

    def test_call_is_write(self):
        assert _is_write_query("CALL my_procedure()")

    def test_select_with_comment_is_not_write(self):
        assert not _is_write_query("-- insert comment\nSELECT 1")

    def test_block_comment_stripped(self):
        assert not _is_write_query("/* DELETE this */ SELECT 1")

    def test_case_insensitive_insert(self):
        assert _is_write_query("insert into foo values (1)")

    def test_case_insensitive_delete(self):
        assert _is_write_query("DELETE FROM foo")

    def test_with_cte_select(self):
        assert not _is_write_query("WITH cte AS (SELECT 1) SELECT * FROM cte")


class TestSanitizeError:
    def test_hides_file_paths(self):
        exc = Exception('File "/home/secret/app.py", line 42, in run')
        result = _sanitize_error(exc)
        assert "/home/secret" not in result
        assert "<hidden>" in result

    def test_truncates_long_messages(self):
        exc = Exception("x" * 1000)
        result = _sanitize_error(exc)
        assert len(result) <= 420
        assert "truncated" in result

    def test_short_messages_pass_through(self):
        exc = Exception("table not found")
        assert "table not found" in _sanitize_error(exc)

    def test_exactly_400_chars_not_truncated(self):
        exc = Exception("a" * 400)
        result = _sanitize_error(exc)
        assert "truncated" not in result

    def test_401_chars_truncated(self):
        exc = Exception("a" * 401)
        result = _sanitize_error(exc)
        assert "truncated" in result

    def test_multiple_file_paths_hidden(self):
        exc = Exception('File "/a/b.py" and File "/c/d.py"')
        result = _sanitize_error(exc)
        assert "/a/b" not in result
        assert "/c/d" not in result


class TestCheckTableAccess:
    def test_no_filter_permits_all(self):
        assert _check_table_access("anything", None, None) is None

    def test_allowed_tables_permits_listed(self):
        assert _check_table_access("users", ["users", "orders"], None) is None

    def test_allowed_tables_blocks_unlisted(self):
        result = _check_table_access("secrets", ["users"], None)
        assert result is not None
        assert "not in allowed_tables" in result

    def test_blocked_tables_blocks_listed(self):
        result = _check_table_access("secrets", None, ["secrets"])
        assert result is not None
        assert "blocked" in result

    def test_blocked_tables_permits_others(self):
        assert _check_table_access("users", None, ["secrets"]) is None

    def test_case_insensitive_allowed(self):
        assert _check_table_access("Users", ["users"], None) is None

    def test_case_insensitive_blocked(self):
        result = _check_table_access("SECRETS", None, ["secrets"])
        assert result is not None

    def test_both_allowed_and_blocked(self):
        # allowed takes precedence check: if in allowed but also in blocked → blocked wins
        result = _check_table_access("users", ["users"], ["users"])
        # allowed passes first, then blocked check fires
        assert result is not None and "blocked" in result

    def test_empty_allowed_list_blocks_all(self):
        result = _check_table_access("users", [], None)
        assert result is not None

    def test_empty_blocked_list_blocks_nothing(self):
        assert _check_table_access("users", None, []) is None


class TestRowsToMarkdown:
    def test_empty_rows_returns_no_rows_message(self):
        result = _rows_to_markdown(["id", "name"], [])
        assert "no rows" in result

    def test_single_row(self):
        result = _rows_to_markdown(["id", "name"], [(1, "Alice")])
        assert "Alice" in result
        assert "id" in result
        assert "name" in result

    def test_pipe_delimiter_present(self):
        result = _rows_to_markdown(["a", "b"], [(1, 2)])
        assert "|" in result

    def test_separator_row_present(self):
        result = _rows_to_markdown(["id"], [(1,)])
        assert "-" in result

    def test_line_count(self):
        result = _rows_to_markdown(["id", "name"], [(1, "Alice"), (2, "Bob")])
        lines = result.splitlines()
        assert len(lines) == 4  # header + separator + 2 data rows

    def test_none_values_rendered_as_string(self):
        result = _rows_to_markdown(["id", "val"], [(1, None)])
        assert "None" in result

    def test_wide_value_expands_column(self):
        result = _rows_to_markdown(["x"], [("a" * 50,)])
        assert "a" * 50 in result

    def test_column_header_wider_than_data(self):
        result = _rows_to_markdown(["very_long_column_name"], [(1,)])
        assert "very_long_column_name" in result

    def test_multiple_columns(self):
        result = _rows_to_markdown(["a", "b", "c"], [(1, 2, 3)])
        assert result.count("|") >= 2


# ===========================================================================
# StrandsSQL class tests
# ===========================================================================


class TestStrandsSQLInit:
    def test_raises_without_connection_string(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        with pytest.raises(ValueError, match="No connection string"):
            StrandsSQL()

    def test_accepts_connection_string(self, db_url):
        db = StrandsSQL(db_url)
        assert db._connection_string == db_url

    def test_falls_back_to_env_var(self, monkeypatch, db_url):
        monkeypatch.setenv("DATABASE_URL", db_url)
        db = StrandsSQL()
        assert db._connection_string == db_url

    def test_default_read_only_true(self, db_url):
        db = StrandsSQL(db_url)
        assert db.read_only is True

    def test_default_max_rows(self, db_url):
        db = StrandsSQL(db_url)
        assert db.max_rows == 500

    def test_default_timeout(self, db_url):
        db = StrandsSQL(db_url)
        assert db.timeout == 30

    def test_default_output_format(self, db_url):
        db = StrandsSQL(db_url)
        assert db.output_format == "markdown"

    def test_custom_settings(self, db_url):
        db = StrandsSQL(
            db_url,
            read_only=False,
            max_rows=100,
            timeout=60,
            output_format="json",
            allowed_tables=["users"],
            blocked_tables=["secrets"],
        )
        assert db.read_only is False
        assert db.max_rows == 100
        assert db.timeout == 60
        assert db.output_format == "json"
        assert db.allowed_tables == ["users"]
        assert db.blocked_tables == ["secrets"]


class TestStrandsSQLListTables:
    def test_lists_all_tables(self, db_url):
        db = StrandsSQL(db_url)
        result = db.list_tables()
        assert "users" in result
        assert "orders" in result

    def test_respects_allowlist(self, db_url):
        db = StrandsSQL(db_url, allowed_tables=["users"])
        result = db.list_tables()
        assert "users" in result
        assert "orders" not in result

    def test_respects_blocklist(self, db_url):
        db = StrandsSQL(db_url, blocked_tables=["orders"])
        result = db.list_tables()
        assert "orders" not in result
        assert "users" in result

    def test_empty_db_no_tables(self, tmp_path):
        url = f"sqlite:///{tmp_path}/empty.db"
        engine = create_engine(url)
        engine.connect().close()
        db = StrandsSQL(url)
        result = db.list_tables()
        assert "No accessible" in result


class TestStrandsSQLDescribeTable:
    def test_describes_columns(self, db_url):
        db = StrandsSQL(db_url)
        result = db.describe_table("users")
        assert "name" in result
        assert "email" in result

    def test_shows_primary_key(self, db_url):
        db = StrandsSQL(db_url)
        result = db.describe_table("users")
        assert "PK" in result

    def test_shows_foreign_key(self, db_url):
        db = StrandsSQL(db_url)
        result = db.describe_table("orders")
        assert "users" in result  # FK references users

    def test_blocked_table_denied(self, db_url):
        db = StrandsSQL(db_url, blocked_tables=["users"])
        result = db.describe_table("users")
        assert "blocked" in result

    def test_nonexistent_table_returns_error(self, db_url):
        db = StrandsSQL(db_url)
        result = db.describe_table("ghost_table")
        assert "error" in result.lower()

    def test_not_in_allowed_denied(self, db_url):
        db = StrandsSQL(db_url, allowed_tables=["orders"])
        result = db.describe_table("users")
        assert "not in allowed_tables" in result


class TestStrandsSQLSchemaSummary:
    def test_contains_all_tables(self, db_url):
        db = StrandsSQL(db_url)
        result = db.schema_summary()
        assert "users" in result
        assert "orders" in result

    def test_contains_column_names(self, db_url):
        db = StrandsSQL(db_url)
        result = db.schema_summary()
        assert "id" in result
        assert "name" in result

    def test_pk_marked_with_asterisk(self, db_url):
        db = StrandsSQL(db_url)
        result = db.schema_summary()
        assert "*id" in result

    def test_respects_blocklist(self, db_url):
        db = StrandsSQL(db_url, blocked_tables=["orders"])
        result = db.schema_summary()
        assert "orders" not in result


class TestStrandsSQLQuery:
    def test_basic_select(self, db_url):
        db = StrandsSQL(db_url)
        result = db.query("SELECT * FROM users")
        assert "Alice" in result
        assert "Bob" in result

    def test_markdown_output_default(self, db_url):
        db = StrandsSQL(db_url)
        result = db.query("SELECT * FROM users")
        assert "|" in result

    def test_json_output_override(self, db_url):
        db = StrandsSQL(db_url)
        result = db.query("SELECT id, name FROM users", output_format="json")
        parsed = json.loads(result)
        assert isinstance(parsed, list)
        assert parsed[0]["name"] == "Alice"

    def test_max_rows_override(self, db_url):
        db = StrandsSQL(db_url)
        result = db.query("SELECT * FROM users", max_rows=1)
        assert "truncated" in result.lower()

    def test_write_blocked_in_read_only(self, db_url):
        db = StrandsSQL(db_url, read_only=True)
        result = db.query("DELETE FROM users")
        assert "read_only" in result or "Write statement" in result

    def test_filter_by_condition(self, db_url):
        db = StrandsSQL(db_url)
        result = db.query("SELECT * FROM users WHERE id = 1")
        assert "Alice" in result
        assert "Bob" not in result

    def test_join_query(self, db_url):
        db = StrandsSQL(db_url)
        result = db.query(
            "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id"
        )
        assert "Alice" in result
        assert "99.99" in result

    def test_blocked_table_in_query(self, db_url):
        db = StrandsSQL(db_url, blocked_tables=["users"])
        result = db.query("SELECT * FROM users")
        assert "blocked" in result

    def test_not_in_allowed_tables(self, db_url):
        db = StrandsSQL(db_url, allowed_tables=["orders"])
        result = db.query("SELECT * FROM users")
        assert "not in allowed_tables" in result

    def test_invalid_sql_returns_error(self, db_url):
        db = StrandsSQL(db_url)
        result = db.query("SELECT * FROM nonexistent_xyz")
        assert "error" in result.lower()

    def test_empty_result_shows_no_rows(self, db_url):
        db = StrandsSQL(db_url)
        result = db.query("SELECT * FROM users WHERE id = 9999")
        assert "no rows" in result

    def test_instance_output_format_respected(self, db_url):
        db = StrandsSQL(db_url, output_format="json")
        result = db.query("SELECT id FROM users")
        parsed = json.loads(result)
        assert isinstance(parsed, list)

    def test_aggregate_query(self, db_url):
        db = StrandsSQL(db_url)
        result = db.query("SELECT COUNT(*) as cnt FROM users")
        assert "2" in result

    def test_null_value_in_result(self, db_url):
        db = StrandsSQL(db_url)
        result = db.query("SELECT email FROM users WHERE id = 2")
        assert "None" in result or "null" in result.lower()


class TestStrandsSQLExecute:
    def test_blocked_when_read_only(self, db_url):
        db = StrandsSQL(db_url, read_only=True)
        with pytest.raises(PermissionError, match="read_only=False"):
            db.execute("INSERT INTO users VALUES (99, 'Test', NULL)")

    def test_insert_succeeds(self, db_url):
        db = StrandsSQL(db_url, read_only=False)
        result = db.execute("INSERT INTO users VALUES (99, 'Eve', 'eve@example.com')")
        assert "OK" in result

    def test_insert_visible_in_query(self, db_url):
        db = StrandsSQL(db_url, read_only=False)
        db.execute("INSERT INTO users VALUES (99, 'Eve', NULL)")
        result = db.query("SELECT name FROM users WHERE id = 99")
        assert "Eve" in result

    def test_update_row_count(self, db_url):
        db = StrandsSQL(db_url, read_only=False)
        result = db.execute("UPDATE users SET email = 'new@x.com' WHERE id = 1")
        assert "1" in result

    def test_delete_row_count(self, db_url):
        db = StrandsSQL(db_url, read_only=False)
        result = db.execute("DELETE FROM users WHERE id = 1")
        assert "1" in result

    def test_delete_reflected_in_query(self, db_url):
        db = StrandsSQL(db_url, read_only=False)
        db.execute("DELETE FROM users WHERE id = 1")
        result = db.query("SELECT * FROM users WHERE id = 1")
        assert "no rows" in result

    def test_blocked_table_denied(self, db_url):
        db = StrandsSQL(db_url, read_only=False, blocked_tables=["users"])
        result = db.execute("INSERT INTO users VALUES (99, 'X', NULL)")
        assert "blocked" in result

    def test_bad_sql_returns_error(self, db_url):
        db = StrandsSQL(db_url, read_only=False)
        result = db.execute("INSERT INTO nonexistent_table VALUES (1)")
        assert "error" in result.lower()


# ===========================================================================
# Low-level sql_database() tool handler tests
# ===========================================================================


class TestSqlDatabaseToolHandler:

    def test_no_connection_string_no_env(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        result = sql_database({"toolUseId": "t1", "name": "sql_database", "input": {"action": "list_tables"}})
        assert result["status"] == "error"
        assert "DATABASE_URL" in result["content"][0]["text"]

    def test_uses_env_var(self, monkeypatch, db_url):
        monkeypatch.setenv("DATABASE_URL", db_url)
        result = sql_database({"toolUseId": "t1", "name": "sql_database", "input": {"action": "list_tables"}})
        assert result["status"] == "success"

    def test_invalid_action_returns_error(self, db_url):
        result = sql_database(_tool_call("bad_action", connection_string=db_url))
        # model validation should catch this
        assert result["status"] == "error"

    def test_read_only_blocks_execute(self, db_url):
        result = sql_database(_tool_call(
            "execute",
            connection_string=db_url,
            sql="INSERT INTO users VALUES (99, 'X', NULL)",
            read_only=True,
        ))
        assert result["status"] == "error"
        assert "blocked" in result["content"][0]["text"]

    def test_read_only_blocks_write_in_query(self, db_url):
        result = sql_database(_tool_call(
            "query",
            connection_string=db_url,
            sql="DELETE FROM users",
            read_only=True,
        ))
        assert result["status"] == "error"

    def test_list_tables_success(self, db_url):
        result = sql_database(_tool_call("list_tables", connection_string=db_url))
        assert result["status"] == "success"
        assert "users" in result["content"][0]["text"]

    def test_list_tables_allowlist(self, db_url):
        result = sql_database(_tool_call("list_tables", connection_string=db_url, allowed_tables=["users"]))
        text = result["content"][0]["text"]
        assert "users" in text
        assert "orders" not in text

    def test_list_tables_blocklist(self, db_url):
        result = sql_database(_tool_call("list_tables", connection_string=db_url, blocked_tables=["orders"]))
        text = result["content"][0]["text"]
        assert "orders" not in text

    def test_describe_table_success(self, db_url):
        result = sql_database(_tool_call("describe_table", connection_string=db_url, table="users"))
        assert result["status"] == "success"
        assert "PK" in result["content"][0]["text"]

    def test_describe_table_missing_table_param(self, db_url):
        result = sql_database({"toolUseId": "t1", "name": "sql_database", "input": {
            "action": "describe_table", "connection_string": db_url
        }})
        assert result["status"] == "error"

    def test_describe_nonexistent_table(self, db_url):
        result = sql_database(_tool_call("describe_table", connection_string=db_url, table="ghost"))
        assert "error" in result["content"][0]["text"].lower()

    def test_describe_blocked_table(self, db_url):
        result = sql_database(_tool_call(
            "describe_table", connection_string=db_url, table="users", blocked_tables=["users"]
        ))
        assert "blocked" in result["content"][0]["text"]

    def test_schema_summary_success(self, db_url):
        result = sql_database(_tool_call("schema_summary", connection_string=db_url))
        assert result["status"] == "success"
        assert "users" in result["content"][0]["text"]

    def test_query_select(self, db_url):
        result = sql_database(_tool_call("query", connection_string=db_url, sql="SELECT * FROM users"))
        assert result["status"] == "success"
        assert "Alice" in result["content"][0]["text"]

    def test_query_markdown(self, db_url):
        result = sql_database(_tool_call(
            "query", connection_string=db_url, sql="SELECT * FROM users", output_format="markdown"
        ))
        assert "|" in result["content"][0]["text"]

    def test_query_json(self, db_url):
        result = sql_database(_tool_call(
            "query", connection_string=db_url, sql="SELECT id, name FROM users", output_format="json"
        ))
        parsed = json.loads(result["content"][0]["text"])
        assert isinstance(parsed, list)
        assert parsed[0]["name"] == "Alice"

    def test_query_max_rows_truncation(self, db_url):
        result = sql_database(_tool_call(
            "query", connection_string=db_url, sql="SELECT * FROM users", max_rows=1
        ))
        assert "truncated" in result["content"][0]["text"].lower()

    def test_query_bad_sql(self, db_url):
        result = sql_database(_tool_call(
            "query", connection_string=db_url, sql="SELECT * FROM does_not_exist"
        ))
        assert "error" in result["content"][0]["text"].lower()

    def test_query_blocked_table(self, db_url):
        result = sql_database(_tool_call(
            "query", connection_string=db_url, sql="SELECT * FROM users", blocked_tables=["users"]
        ))
        assert "blocked" in result["content"][0]["text"]

    def test_query_not_in_allowed(self, db_url):
        result = sql_database(_tool_call(
            "query", connection_string=db_url, sql="SELECT * FROM orders", allowed_tables=["users"]
        ))
        assert "not in allowed_tables" in result["content"][0]["text"]

    def test_query_missing_sql(self, db_url):
        result = sql_database({"toolUseId": "t1", "name": "sql_database", "input": {
            "action": "query", "connection_string": db_url
        }})
        assert result["status"] == "error"

    def test_execute_write_success(self, db_url):
        result = sql_database(_tool_call(
            "execute",
            connection_string=db_url,
            sql="INSERT INTO users VALUES (99, 'Test', NULL)",
            read_only=False,
        ))
        assert result["status"] == "success"
        assert "OK" in result["content"][0]["text"]

    def test_execute_reflected_in_query(self, db_url):
        sql_database(_tool_call(
            "execute",
            connection_string=db_url,
            sql="INSERT INTO users VALUES (99, 'Charlie', NULL)",
            read_only=False,
        ))
        result = sql_database(_tool_call(
            "query", connection_string=db_url, sql="SELECT name FROM users WHERE id=99"
        ))
        assert "Charlie" in result["content"][0]["text"]

    def test_execute_rowcount_reported(self, db_url):
        result = sql_database(_tool_call(
            "execute",
            connection_string=db_url,
            sql="UPDATE users SET email='x@x.com' WHERE id=1",
            read_only=False,
        ))
        assert "1" in result["content"][0]["text"]

    def test_execute_missing_sql(self, db_url):
        result = sql_database({"toolUseId": "t1", "name": "sql_database", "input": {
            "action": "execute", "connection_string": db_url, "read_only": False
        }})
        assert result["status"] == "error"

    def test_execute_blocked_table(self, db_url):
        result = sql_database(_tool_call(
            "execute",
            connection_string=db_url,
            sql="INSERT INTO users VALUES (99, 'X', NULL)",
            read_only=False,
            blocked_tables=["users"],
        ))
        assert "blocked" in result["content"][0]["text"]

    def test_invalid_connection_string(self):
        result = sql_database(_tool_call(
            "list_tables", connection_string="postgresql://bad:bad@localhost:9999/nodb"
        ))
        # Should either fail at connect or return error text
        text = result["content"][0]["text"]
        assert "error" in text.lower() or result["status"] == "error"


# ===========================================================================
# Engine cache tests
# ===========================================================================


class TestEngineCache:
    def test_engine_is_cached(self, db_url):
        _ENGINE_CACHE.clear()
        e1 = _get_engine(db_url, 30)
        e2 = _get_engine(db_url, 30)
        assert e1 is e2

    def test_different_urls_get_different_engines(self, db_url, tmp_path):
        db2 = f"sqlite:///{tmp_path}/other.db"
        e1 = _get_engine(db_url, 30)
        e2 = _get_engine(db2, 30)
        assert e1 is not e2

    def test_cache_key_is_connection_string(self, db_url):
        _ENGINE_CACHE.clear()
        _get_engine(db_url, 30)
        assert db_url in _ENGINE_CACHE


# ===========================================================================
# Model validation tests
# ===========================================================================


class TestSqlDatabaseInput:
    def test_valid_list_tables(self):
        m = SqlDatabaseInput(action="list_tables")
        assert m.sql is None
        assert m.table is None

    def test_valid_schema_summary(self):
        m = SqlDatabaseInput(action="schema_summary")
        assert m.sql is None

    def test_valid_query(self):
        m = SqlDatabaseInput(action="query", sql="SELECT 1")
        assert m.max_rows == 500
        assert m.read_only is True
        assert m.output_format == "markdown"
        assert m.timeout == 30

    def test_valid_execute(self):
        m = SqlDatabaseInput(action="execute", sql="INSERT INTO t VALUES (1)", read_only=False)
        assert m.sql is not None

    def test_valid_describe_table(self):
        m = SqlDatabaseInput(action="describe_table", table="users")
        assert m.table == "users"

    def test_query_requires_sql(self):
        with pytest.raises(Exception, match="sql"):
            SqlDatabaseInput(action="query")

    def test_execute_requires_sql(self):
        with pytest.raises(Exception, match="sql"):
            SqlDatabaseInput(action="execute")

    def test_describe_requires_table(self):
        with pytest.raises(Exception, match="table"):
            SqlDatabaseInput(action="describe_table")

    def test_max_rows_lower_bound_zero(self):
        with pytest.raises(Exception):
            SqlDatabaseInput(action="query", sql="SELECT 1", max_rows=0)

    def test_max_rows_lower_bound_negative(self):
        with pytest.raises(Exception):
            SqlDatabaseInput(action="query", sql="SELECT 1", max_rows=-1)

    def test_max_rows_upper_bound(self):
        with pytest.raises(Exception):
            SqlDatabaseInput(action="query", sql="SELECT 1", max_rows=10_001)

    def test_max_rows_valid_min(self):
        m = SqlDatabaseInput(action="query", sql="SELECT 1", max_rows=1)
        assert m.max_rows == 1

    def test_max_rows_valid_max(self):
        m = SqlDatabaseInput(action="query", sql="SELECT 1", max_rows=10_000)
        assert m.max_rows == 10_000

    def test_timeout_lower_bound_zero(self):
        with pytest.raises(Exception):
            SqlDatabaseInput(action="query", sql="SELECT 1", timeout=0)

    def test_timeout_lower_bound_negative(self):
        with pytest.raises(Exception):
            SqlDatabaseInput(action="query", sql="SELECT 1", timeout=-1)

    def test_timeout_upper_bound(self):
        with pytest.raises(Exception):
            SqlDatabaseInput(action="query", sql="SELECT 1", timeout=301)

    def test_timeout_valid_min(self):
        m = SqlDatabaseInput(action="query", sql="SELECT 1", timeout=1)
        assert m.timeout == 1

    def test_timeout_valid_max(self):
        m = SqlDatabaseInput(action="query", sql="SELECT 1", timeout=300)
        assert m.timeout == 300

    def test_output_format_default_markdown(self):
        m = SqlDatabaseInput(action="query", sql="SELECT 1")
        assert m.output_format == "markdown"

    def test_output_format_json(self):
        m = SqlDatabaseInput(action="query", sql="SELECT 1", output_format="json")
        assert m.output_format == "json"

    def test_output_format_invalid(self):
        with pytest.raises(Exception):
            SqlDatabaseInput(action="query", sql="SELECT 1", output_format="csv")

    def test_output_format_invalid_xml(self):
        with pytest.raises(Exception):
            SqlDatabaseInput(action="query", sql="SELECT 1", output_format="xml")

    def test_allowed_tables_stored(self):
        m = SqlDatabaseInput(action="query", sql="SELECT 1", allowed_tables=["users", "orders"])
        assert m.allowed_tables == ["users", "orders"]

    def test_blocked_tables_stored(self):
        m = SqlDatabaseInput(action="query", sql="SELECT 1", blocked_tables=["secrets"])
        assert m.blocked_tables == ["secrets"]

    def test_both_allowed_and_blocked_tables(self):
        m = SqlDatabaseInput(
            action="query",
            sql="SELECT 1",
            allowed_tables=["users"],
            blocked_tables=["secrets"],
        )
        assert m.allowed_tables == ["users"]
        assert m.blocked_tables == ["secrets"]

    def test_read_only_defaults_true(self):
        m = SqlDatabaseInput(action="query", sql="SELECT 1")
        assert m.read_only is True

    def test_read_only_can_be_false(self):
        m = SqlDatabaseInput(action="execute", sql="DELETE FROM t", read_only=False)
        assert m.read_only is False

    def test_connection_string_optional(self):
        m = SqlDatabaseInput(action="list_tables")
        assert m.connection_string is None

    def test_connection_string_stored(self):
        m = SqlDatabaseInput(action="list_tables", connection_string="sqlite:///./x.db")
        assert m.connection_string == "sqlite:///./x.db"

    def test_invalid_action(self):
        with pytest.raises(Exception):
            SqlDatabaseInput(action="drop_everything")