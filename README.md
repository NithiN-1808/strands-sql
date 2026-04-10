# strands-sql
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