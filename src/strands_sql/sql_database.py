"""
strands-sql — General-purpose SQL tool for Strands agents.

Supports PostgreSQL, MySQL, and SQLite via SQLAlchemy.
"""

from __future__ import annotations

import os
import re
import textwrap
from typing import Any

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool
from strands.types.tools import ToolResult, ToolUse
import sqlglot
from sqlglot import exp
from .models import SqlDatabaseInput

# ---------------------------------------------------------------------------
# Engine cache — one engine per connection string, reused across tool calls
# ---------------------------------------------------------------------------
_ENGINE_CACHE: dict[str, Engine] = {}


def _get_engine(connection_string: str, timeout: int) -> Engine:
    key = connection_string
    if key not in _ENGINE_CACHE:
        _ENGINE_CACHE[key] = create_engine(
            connection_string,
            poolclass=NullPool,
            pool_pre_ping=True,
            connect_args=_timeout_args(connection_string, timeout),
        )
    return _ENGINE_CACHE[key]


def _timeout_args(connection_string: str, timeout: int) -> dict:
    """Return driver-specific timeout connect_args."""
    cs = connection_string.lower()
    if cs.startswith("postgresql") or cs.startswith("postgres"):
        return {"connect_timeout": timeout, "options": f"-c statement_timeout={timeout * 1000}"}
    if cs.startswith("mysql"):
        return {"connect_timeout": timeout, "read_timeout": timeout, "write_timeout": timeout}
    return {}


# ---------------------------------------------------------------------------
# Security helpers
# ---------------------------------------------------------------------------
_WRITE_PATTERN = re.compile(
    r"^\s*(insert|update|delete|drop|create|alter|truncate|replace|merge|call|exec)\b",
    re.IGNORECASE,
)
_COMMENT_PATTERN = re.compile(r"(--[^\n]*|/\*.*?\*/)", re.DOTALL)


def _is_write_query(sql: str) -> bool:
    # Strip line comments and block comments before checking
    cleaned = re.sub(r"--[^\n]*", "", sql)
    cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
    cleaned = cleaned.strip()
    return bool(
        re.match(
            r"(insert|update|delete|drop|create|alter|truncate|replace|merge|call|exec)\b",
            cleaned,
            re.IGNORECASE,
        )
    )


def _sanitize_error(exc: Exception) -> str:
    """Return a safe error message that doesn't leak internal stack details."""
    msg = str(exc)
    msg = re.sub(r'File ".*?"', 'File "<hidden>"', msg)
    if len(msg) > 400:
        msg = msg[:400] + "... [truncated]"
    return msg

def _extract_tables(sql: str) -> set[str]:
    try:
        parsed = sqlglot.parse_one(sql)
        return {table.name for table in parsed.find_all(exp.Table)}
    except Exception:
        return set()


def _check_table_access(
    table: str,
    allowed_tables: list[str] | None,
    blocked_tables: list[str] | None,
) -> str | None:
    if allowed_tables is not None and table.lower() not in [t.lower() for t in allowed_tables]:
        return f"Access denied: table '{table}' is not in allowed_tables."
    if blocked_tables and table.lower() in [t.lower() for t in blocked_tables]:
        return f"Access denied: table '{table}' is blocked."
    return None


def _check_sql_table_access(
    sql: str,
    allowed_tables: list[str] | None,
    blocked_tables: list[str] | None,
) -> str | None:
    if not allowed_tables and not blocked_tables:
        return None

    tables = _extract_tables(sql)

    for tbl in tables:
        err = _check_table_access(tbl, allowed_tables, blocked_tables)
        if err:
            return err

    return None


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def _rows_to_markdown(columns: list[str], rows: list[tuple[Any, ...]]) -> str:
    if not rows:
        return "*(no rows)*"
    col_widths = [
        max(len(c), max((len(str(r[i])) for r in rows), default=0)) for i, c in enumerate(columns)
    ]
    header = " | ".join(c.ljust(col_widths[i]) for i, c in enumerate(columns))
    separator = "-+-".join("-" * w for w in col_widths)
    data_rows = [
        " | ".join(str(row[i]).ljust(col_widths[i]) for i in range(len(columns))) for row in rows
    ]
    return "\n".join([header, separator] + data_rows)


def _format_results(
    columns: list[str],
    rows: list[tuple[Any, ...]],
    output_format: str,
    max_rows: int,
    truncated: bool,
) -> str:
    note = f"\n\n⚠️  Results truncated to {max_rows} rows." if truncated else ""
    if output_format == "markdown":
        return _rows_to_markdown(columns, rows) + note
    import json
    result = [dict(zip(columns, row)) for row in rows]
    return json.dumps(result, default=str, indent=2) + note


# ---------------------------------------------------------------------------
# Action implementations (internal)
# ---------------------------------------------------------------------------

def _list_tables(
    engine: Engine,
    allowed_tables: list[str] | None,
    blocked_tables: list[str] | None,
) -> str:
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    views = inspector.get_view_names()
    all_objects = [("table", t) for t in tables] + [("view", v) for v in views]

    filtered = []
    for kind, name in all_objects:
        if allowed_tables and name.lower() not in [t.lower() for t in allowed_tables]:
            continue
        if blocked_tables and name.lower() in [t.lower() for t in blocked_tables]:
            continue
        filtered.append((kind, name))

    if not filtered:
        return "No accessible tables or views found."
    return "\n".join(f"[{kind}] {name}" for kind, name in filtered)


def _describe_table(
    engine: Engine,
    table: str,
    allowed_tables: list[str] | None,
    blocked_tables: list[str] | None,
) -> str:
    err = _check_table_access(table, allowed_tables, blocked_tables)
    if err:
        return err

    inspector = inspect(engine)
    try:
        columns = inspector.get_columns(table)
        pk_info = inspector.get_pk_constraint(table)
        fk_info = inspector.get_foreign_keys(table)
    except Exception as exc:
        return f"Error describing table: {_sanitize_error(exc)}"

    pk_cols = set(pk_info.get("constrained_columns", []))
    lines = [f"Table: {table}", "", "Columns:"]
    for col in columns:
        flags = []
        if col["name"] in pk_cols:
            flags.append("PK")
        if not col.get("nullable", True):
            flags.append("NOT NULL")
        flag_str = "  [" + ", ".join(flags) + "]" if flags else ""
        default = f"  default={col['default']}" if col.get("default") is not None else ""
        lines.append(f"  {col['name']}  {col['type']}{flag_str}{default}")

    if fk_info:
        lines += ["", "Foreign Keys:"]
        for fk in fk_info:
            cols = ", ".join(fk["constrained_columns"])
            ref_table = fk["referred_table"]
            ref_cols = ", ".join(fk["referred_columns"])
            lines.append(f"  ({cols}) → {ref_table}({ref_cols})")

    return "\n".join(lines)


def _schema_summary(
    engine: Engine,
    allowed_tables: list[str] | None,
    blocked_tables: list[str] | None,
    max_tables: int = 30,
) -> str:
    inspector = inspect(engine)
    all_tables = inspector.get_table_names()

    visible = []
    for t in all_tables:
        if allowed_tables and t.lower() not in [x.lower() for x in allowed_tables]:
            continue
        if blocked_tables and t.lower() in [x.lower() for x in blocked_tables]:
            continue
        visible.append(t)

    truncated_tables = len(visible) > max_tables
    visible = visible[:max_tables]

    lines = []
    for table in visible:
        try:
            columns = inspector.get_columns(table)
            pk_info = inspector.get_pk_constraint(table)
            pk_cols = set(pk_info.get("constrained_columns", []))
            col_parts = []
            for col in columns:
                marker = "*" if col["name"] in pk_cols else ""
                col_parts.append(f"{marker}{col['name']}:{col['type']}")
            lines.append(f"{table}({', '.join(col_parts)})")
        except Exception:
            lines.append(f"{table}(schema unavailable)")

    summary = "\n".join(lines)
    if truncated_tables:
        summary += f"\n\n... (showing {max_tables} of {len(all_tables)} tables)"
    return summary


def _run_query(
    engine: Engine,
    sql: str,
    allowed_tables: list[str] | None,
    blocked_tables: list[str] | None,
    max_rows: int,
    output_format: str,
    timeout: int,
) -> str:
    err = _check_sql_table_access(sql, allowed_tables, blocked_tables)
    if err:
        return err

    try:
        sql = sql.strip()
        with engine.connect() as conn:
            result = conn.execute(text(sql).execution_options(timeout=timeout))
            columns = list(result.keys())
            rows: list[tuple[Any, ...]] = [tuple(row) for row in result.fetchmany(max_rows + 1)]
            truncated = len(rows) > max_rows
            rows = rows[:max_rows]
            return _format_results(columns, rows, output_format, max_rows, truncated)
    except Exception as exc:
        return f"Query error: {_sanitize_error(exc)}"


def _run_execute(
    engine: Engine,
    sql: str,
    allowed_tables: list[str] | None,
    blocked_tables: list[str] | None,
    timeout: int,
) -> str:
    err = _check_sql_table_access(sql, allowed_tables, blocked_tables)
    if err:
        return err

    try:
        with engine.begin() as conn:
            result = conn.execute(text(sql).execution_options(timeout=timeout))
            rowcount = result.rowcount
            return f"OK. Rows affected: {rowcount if rowcount >= 0 else 'unknown'}"
    except Exception as exc:
        return f"Execute error: {_sanitize_error(exc)}"


# ---------------------------------------------------------------------------
# StrandsSQL — the primary public API
# ---------------------------------------------------------------------------

class StrandsSQL:
    """
    A clean, class-based interface to the strands-sql tool.

    Set the connection once; call methods directly or pass to a Strands agent.

    Example::

        db = StrandsSQL("sqlite:///./local.db")

        # Direct usage
        print(db.list_tables())
        print(db.query("SELECT * FROM users"))

        # Agent usage
        from strands import Agent
        agent = Agent(tools=[db.as_tool()])
        agent("How many users are there?")
    """

    def __init__(
        self,
        connection_string: str | None = None,
        *,
        read_only: bool = True,
        max_rows: int = 500,
        timeout: int = 30,
        output_format: str = "markdown",
        allowed_tables: list[str] | None = None,
        blocked_tables: list[str] | None = None,
    ) -> None:
        """
        Parameters
        ----------
        connection_string:
            SQLAlchemy URL (e.g. ``"sqlite:///./local.db"``).
            Falls back to the ``DATABASE_URL`` environment variable.
        read_only:
            Block all write queries when ``True`` (default).
        max_rows:
            Maximum rows returned by :meth:`query`. Default 500.
        timeout:
            Query timeout in seconds (1–300). Default 30.
        output_format:
            ``"markdown"`` (default) or ``"json"``.
        allowed_tables:
            Allowlist — only these tables are accessible.
        blocked_tables:
            Blocklist — these tables are never accessible.
        """
        self._connection_string = connection_string or os.environ.get("DATABASE_URL")
        if not self._connection_string:
            raise ValueError(
                "No connection string provided. "
                "Pass one to StrandsSQL() or set the DATABASE_URL environment variable."
            )
        self.read_only = read_only
        self.max_rows = max_rows
        self.timeout = timeout
        self.output_format = output_format
        self.allowed_tables = allowed_tables
        self.blocked_tables = blocked_tables

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @property
    def _engine(self) -> Engine:
        return _get_engine(self._connection_string, self.timeout)

    def _defaults(self, **overrides) -> dict:
        """Merge instance defaults with per-call overrides."""
        base = dict(
            connection_string=self._connection_string,
            read_only=self.read_only,
            max_rows=self.max_rows,
            timeout=self.timeout,
            output_format=self.output_format,
            allowed_tables=self.allowed_tables,
            blocked_tables=self.blocked_tables,
        )
        base.update({k: v for k, v in overrides.items() if v is not None})
        return base

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def list_tables(self) -> str:
        """List all accessible tables and views."""
        return _list_tables(self._engine, self.allowed_tables, self.blocked_tables)

    def describe_table(self, table: str) -> str:
        """Describe columns, types, primary keys, and foreign keys for *table*."""
        return _describe_table(self._engine, table, self.allowed_tables, self.blocked_tables)

    def schema_summary(self) -> str:
        """Return a compact schema of all accessible tables — ideal as LLM context."""
        return _schema_summary(self._engine, self.allowed_tables, self.blocked_tables)

    def query(
        self,
        sql: str,
        *,
        output_format: str | None = None,
        max_rows: int | None = None,
    ) -> str:
        """
        Run a SELECT statement and return results.

        Parameters
        ----------
        sql:
            A SELECT query.
        output_format:
            Override the instance default (``"markdown"`` or ``"json"``).
        max_rows:
            Override the instance default row cap.
        """
        if self.read_only and _is_write_query(sql):
            return (
                "Write statement detected in read_only mode. "
                "Use execute() with read_only=False."
            )
        return _run_query(
            self._engine,
            sql,
            self.allowed_tables,
            self.blocked_tables,
            max_rows if max_rows is not None else self.max_rows,
            output_format if output_format is not None else self.output_format,
            self.timeout,
        )

    def execute(self, sql: str) -> str:
        """
        Run a write statement (INSERT / UPDATE / DELETE / DDL).

        Raises ``PermissionError`` if the instance was created with ``read_only=True``.
        """
        if self.read_only:
            raise PermissionError(
                "Write queries are blocked. Create StrandsSQL with read_only=False to enable."
            )
        return _run_execute(
            self._engine,
            sql,
            self.allowed_tables,
            self.blocked_tables,
            self.timeout,
        )

    def as_tool(self):
        """
        Return a Strands-compatible Tool bound to this instance's connection and settings.

        Example::

            from strands import Agent
            db = StrandsSQL("sqlite:///./local.db")
            agent = Agent(tools=[db.as_tool()])
            agent("List all tables")
        """
        from strands.tools import Tool

        instance = self  # capture for closure

        def _bound_sql_database(tool: ToolUse, **kwargs: Any) -> ToolResult:
            tool_input = dict(tool.get("input", {}))
            # Inject instance-level defaults so the agent never needs to supply them
            tool_input.setdefault("connection_string", instance._connection_string)
            tool_input.setdefault("read_only", instance.read_only)
            tool_input.setdefault("max_rows", instance.max_rows)
            tool_input.setdefault("timeout", instance.timeout)
            tool_input.setdefault("output_format", instance.output_format)
            if instance.allowed_tables is not None:
                tool_input.setdefault("allowed_tables", instance.allowed_tables)
            if instance.blocked_tables is not None:
                tool_input.setdefault("blocked_tables", instance.blocked_tables)
            modified_tool = dict(tool)
            modified_tool["input"] = tool_input
            return sql_database(modified_tool, **kwargs)

        return Tool.from_function(
            func=_bound_sql_database,
            name=TOOL_SPEC["name"],
            description=TOOL_SPEC["description"],
            input_schema=TOOL_SPEC["inputSchema"]["json"],
        )


# ---------------------------------------------------------------------------
# TOOL_SPEC + low-level tool handler (used by StrandsSQL.as_tool and get_tool)
# ---------------------------------------------------------------------------

TOOL_SPEC = {
    "name": "sql_database",
    "description": textwrap.dedent("""\
        General-purpose SQL tool. Supports list_tables, describe_table,
        schema_summary, query (SELECT), and execute (write, if enabled).
        Works with PostgreSQL, MySQL, and SQLite via SQLAlchemy.
    """),
    "inputSchema": {
        "json": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["list_tables", "describe_table", "schema_summary", "query", "execute"],
                    "description": "The action to perform.",
                },
                "sql": {
                    "type": "string",
                    "description": "SQL string for 'query' or 'execute' actions.",
                },
                "table": {
                    "type": "string",
                    "description": "Table name for 'describe_table' action.",
                },
                "connection_string": {
                    "type": "string",
                    "description": "SQLAlchemy connection string. Falls back to DATABASE_URL env var.",
                },
                "read_only": {
                    "type": "boolean",
                    "description": "Block write queries. Default true.",
                },
                "max_rows": {
                    "type": "integer",
                    "description": "Max rows returned by 'query'. Default 500.",
                },
                "timeout": {
                    "type": "integer",
                    "description": "Query timeout in seconds. Default 30.",
                },
                "output_format": {
                    "type": "string",
                    "enum": ["json", "markdown"],
                    "description": "Output format for query results. Default 'markdown'.",
                },
                "allowed_tables": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Allowlist of table names the agent may access.",
                },
                "blocked_tables": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Blocklist of table names the agent may not access.",
                },
            },
            "required": ["action"],
        }
    },
}


def sql_database(tool: ToolUse, **kwargs: Any) -> ToolResult:
    """Low-level Strands tool handler. Prefer StrandsSQL for direct usage."""
    tool_input = tool.get("input", {})

    try:
        params = SqlDatabaseInput(**tool_input)
    except Exception as exc:
        return {
            "toolUseId": tool["toolUseId"],
            "status": "error",
            "content": [{"text": f"Invalid input: {exc}"}],
        }

    connection_string = params.connection_string or os.environ.get("DATABASE_URL")
    if not connection_string:
        return {
            "toolUseId": tool["toolUseId"],
            "status": "error",
            "content": [
                {"text": "No connection string provided. Set DATABASE_URL or pass connection_string."}
            ],
        }

    if params.read_only and params.action == "execute":
        return {
            "toolUseId": tool["toolUseId"],
            "status": "error",
            "content": [{"text": "Write queries are blocked. Set read_only=False to enable execute."}],
        }

    if params.read_only and params.action == "query" and params.sql and _is_write_query(params.sql):
        return {
            "toolUseId": tool["toolUseId"],
            "status": "error",
            "content": [
                {
                    "text": (
                        "Write statement detected in read_only mode. "
                        "Use action='execute' with read_only=False."
                    )
                }
            ],
        }

    try:
        engine = _get_engine(connection_string, params.timeout)
    except Exception as exc:
        return {
            "toolUseId": tool["toolUseId"],
            "status": "error",
            "content": [{"text": f"Connection failed: {_sanitize_error(exc)}"}],
        }

    try:
        if params.action == "list_tables":
            result = _list_tables(engine, params.allowed_tables, params.blocked_tables)

        elif params.action == "describe_table":
            if not params.table:
                result = "Error: 'table' parameter is required for describe_table."
            else:
                result = _describe_table(
                    engine, params.table, params.allowed_tables, params.blocked_tables
                )

        elif params.action == "schema_summary":
            result = _schema_summary(engine, params.allowed_tables, params.blocked_tables)

        elif params.action == "query":
            if not params.sql:
                result = "Error: 'sql' parameter is required for query."
            else:
                result = _run_query(
                    engine,
                    params.sql,
                    params.allowed_tables,
                    params.blocked_tables,
                    params.max_rows,
                    params.output_format,
                    params.timeout,
                )

        elif params.action == "execute":
            if not params.sql:
                result = "Error: 'sql' parameter is required for execute."
            else:
                result = _run_execute(
                    engine,
                    params.sql,
                    params.allowed_tables,
                    params.blocked_tables,
                    params.timeout,
                )
        else:
            result = f"Unknown action: {params.action}"

    except Exception as exc:
        result = f"Unexpected error: {_sanitize_error(exc)}"

    return {
        "toolUseId": tool["toolUseId"],
        "status": "success",
        "content": [{"text": result}],
    }


sql_database.TOOL_SPEC = TOOL_SPEC  # type: ignore[attr-defined]
sql_database.tool_spec = TOOL_SPEC  # type: ignore[attr-defined]


def get_tool():
    """Return a Strands Tool using the DATABASE_URL environment variable."""
    from strands.tools import Tool

    return Tool.from_function(
        func=sql_database,
        name=TOOL_SPEC["name"],
        description=TOOL_SPEC["description"],
        input_schema=TOOL_SPEC["inputSchema"]["json"],
    )


def run_sql_database(**kwargs):
    """Direct usage without needing ToolUse format. Prefer StrandsSQL for new code."""
    result = sql_database(
        tool={
            "toolUseId": "direct",
            "input": kwargs,
        }
    )
    return result["content"][0]["text"]