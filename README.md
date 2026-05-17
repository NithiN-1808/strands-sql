# strands-sql

[![Awesome Strands Agents](https://img.shields.io/badge/Awesome-Strands%20Agents-00FF77?style=flat-square&logo=data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjkwIiBoZWlnaHQ9IjQ2MyIgdmlld0JveD0iMCAwIDI5MCA0NjMiIGZpbGw9Im5vbmUiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyI+CjxwYXRoIGQ9Ik05Ny4yOTAyIDUyLjc4ODRDODUuMDY3NCA0OS4xNjY3IDcyLjIyMzQgNTYuMTM4OSA2OC42MDE3IDY4LjM2MTZDNjQuOTgwMSA4MC41ODQzIDcxLjk1MjQgOTMuNDI4MyA4NC4xNzQ5IDk3LjA1MDFMMjM1LjExNyAxMzkuNzc1QzI0NS4yMjMgMTQyLjc2OSAyNDYuMzU3IDE1Ni42MjggMjM2Ljg3NCAxNjEuMjI2TDMyLjU0NiAyNjAuMjkxQy0xNC45NDM5IDI4My4zMTYgLTkuMTYxMDcgMzUyLjc0IDQxLjQ4MzUgMzY3LjU5MUwxODkuNTUxIDQxMS4wMDlMMTkwLjEyNSA0MTEuMTY5QzIwMi4xODMgNDE0LjM3NiAyMTQuNjY1IDQwNy4zOTYgMjE4LjE5NiAzOTUuMzU1QzIyMS43ODQgMzgzLjEyMiAyMTQuNzc0IDM3MC4yOTYgMjAyLjU0MSAzNjYuNzA5TDU0LjQ3MzggMzIzLjI5MUM0NC4zNDQ3IDMyMC4zMjEgNDMuMTg3OSAzMDYuNDM2IDUyLjY4NTcgMzAxLjgzMUwyNTcuMDE0IDIwMi43NjZDMzA0LjQzMiAxNzkuNzc2IDI5OC43NTggMTEwLjQ4MyAyNDguMjMzIDk1LjUxMkw5Ny4yOTAyIDUyLjc4ODRaIiBmaWxsPSIjRkZGRkZGIi8+CjxwYXRoIGQ9Ik0yNTkuMTQ3IDAuOTgxODEyQzI3MS4zODkgLTIuNTc0OTggMjg0LjE5NyA0LjQ2NTcxIDI4Ny43NTQgMTYuNzA3NEMyOTEuMzExIDI4Ljk0OTIgMjg0LjI3IDQxLjc1NyAyNzIuMDI4IDQ1LjMxMzhMNzEuMTcyNyAxMDMuNjcxQzQwLjcxNDIgMTEyLjUyMSAzNy4xOTc2IDE1NC4yNjIgNjUuNzQ1OSAxNjguMDgzTDI0MS4zNDMgMjUzLjA5M0MzMDcuODcyIDI4NS4zMDIgMjk5Ljc5NCAzODIuNTQ2IDIyOC44NjIgNDAzLjMzNkwzMC40MDQxIDQ2MS41MDJDMTguMTcwNyA0NjUuMDg4IDUuMzQ3MDggNDU4LjA3OCAxLjc2MTUzIDQ0NS44NDRDLTEuODIzOSA0MzMuNjExIDUuMTg2MzcgNDIwLjc4NyAxNy40MTk3IDQxNy4yMDJMMjE1Ljg3OCAzNTkuMDM1QzI0Ni4yNzcgMzUwLjEyNSAyNDkuNzM5IDMwOC40NDkgMjIxLjIyNiAyOTQuNjQ1TDQ1LjYyOTcgMjA5LjYzNUMtMjAuOTgzNCAxNzcuMzg2IC0xMi43NzcyIDc5Ljk4OTMgNTguMjkyOCA1OS4zNDAyTDI1OS4xNDcgMC45ODE4MTJaIiBmaWxsPSIjRkZGRkZGIi8+Cjwvc3ZnPgo=&logoColor=white)](https://github.com/cagataycali/awesome-strands-agents)
[![PyPI](https://img.shields.io/pypi/v/strands-sql)](https://pypi.org/project/strands-sql/)
[![Python](https://img.shields.io/pypi/pyversions/strands-sql)](https://pypi.org/project/strands-sql/)

A general-purpose SQL tool for [Strands Agents](https://strandsagents.com) — supports PostgreSQL, MySQL, and SQLite via SQLAlchemy.

## Installation

```bash
# SQLite (no extra driver needed)
pip install strands-sql

# PostgreSQL
pip install "strands-sql[postgres]"

# MySQL
pip install "strands-sql[mysql]"
```

> `strands-sql` requires `sqlglot` for SQL parsing — it is installed automatically as a dependency.

## Quick Start

```python
from strands_sql import StrandsSQL

db = StrandsSQL("sqlite:///./local.db")

print(db.list_tables())
print(db.schema_summary())
print(db.describe_table("users"))
print(db.query("SELECT * FROM orders WHERE amount > 100"))

# Write data (disabled by default — pass read_only=False to enable)
db_write = StrandsSQL("sqlite:///./local.db", read_only=False)
db_write.execute("INSERT INTO users (name, age) VALUES ('Eve', 22)")
```

## Use with a Strands Agent

```python
from strands import Agent
from strands_sql import StrandsSQL

db = StrandsSQL("sqlite:///./local.db")

# Use db.as_tool() to preserve your connection and settings
agent = Agent(tools=[db.as_tool()])

agent("How many users are there?")
agent("Show me all orders above 100")
agent("What tables exist in this database?")
```

> ⚠️ **Note**  
> Always use `db.as_tool()` rather than passing `sql_database` directly.
> `as_tool()` binds your connection string, `read_only` flag, table access rules,
> and other settings to the tool — passing `sql_database` directly means the agent
> must supply all of these itself on every call.

## Configuration

### Connection String

Pass it to `StrandsSQL()` directly, or set the `DATABASE_URL` environment variable:

```bash
export DATABASE_URL="postgresql://user:password@localhost:5432/mydb"
```

```python
db = StrandsSQL("postgresql://user:password@localhost:5432/mydb")  # explicit
db = StrandsSQL()  # reads DATABASE_URL automatically
```

### Options

```python
db = StrandsSQL(
    "sqlite:///./local.db",
    read_only=True,
    max_rows=500,
    timeout=30,
    output_format="markdown",
    allowed_tables=["users", "orders"],
    blocked_tables=["secrets"],
)
```

| Option | Default | Description |
|---|---|---|
| `read_only` | `True` | Blocks all write queries |
| `max_rows` | `500` | Maximum rows returned by `query()` |
| `timeout` | `30` | Query timeout in seconds (1–300) |
| `output_format` | `"markdown"` | `"markdown"` or `"json"` |
| `allowed_tables` | `None` | Allowlist — only these tables are accessible |
| `blocked_tables` | `None` | Blocklist — these tables are never accessible |

## Methods

### `list_tables()`
List all accessible tables and views.

### `describe_table(table)`
Show columns, types, primary keys, and foreign keys for a table.

### `schema_summary()`
Compact schema of all tables — ideal for giving an LLM context about your database.

### `query(sql, *, output_format=None, max_rows=None)`

Run a SELECT statement. Both `output_format` and `max_rows` can be overridden per-call.
Write queries are blocked when `read_only=True`.

```python
db.query("SELECT * FROM users")                          # markdown (default)
db.query("SELECT * FROM users", output_format="json")   # JSON array
db.query("SELECT * FROM logs", max_rows=100)            # override row cap
```

### `execute(sql)`

Run a write statement (INSERT / UPDATE / DELETE / DDL).

Raises `PermissionError` if `read_only=True`. If `allowed_tables` or `blocked_tables`
are configured, access rules are still enforced and return an error string rather than
raising.

```python
db_write = StrandsSQL("sqlite:///./local.db", read_only=False)
db_write.execute("INSERT INTO users (name, age) VALUES ('Eve', 22)")
db_write.execute("UPDATE users SET age = 30 WHERE name = 'Alice'")
db_write.execute("DELETE FROM users WHERE name = 'Bob'")
```

### `as_tool()`
Return a Strands-compatible tool bound to this instance's settings.

## Output Formats

```python
db.query("SELECT * FROM users", output_format="markdown")  # default
db.query("SELECT * FROM users", output_format="json")
```

## Low-level API

For advanced use cases, two additional functions are available:

- **`get_tool()`** — returns a Strands `Tool` that reads `DATABASE_URL` from the environment at call time. Useful when you don't want to construct a `StrandsSQL` instance.
- **`run_sql_database(**kwargs)`** — calls the tool handler directly without the `ToolUse` wrapper format. Prefer `StrandsSQL` for new code.

## Development

```bash
git clone https://github.com/NithiN-1808/strands-sql
cd strands-sql
pip install -e ".[dev]"
pytest
pytest --cov=strands_sql --cov-report=term-missing
ruff check src/ tests/
mypy src/strands_sql/
```

## License

Apache 2.0