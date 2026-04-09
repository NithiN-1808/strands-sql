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

## Quick Start

### Direct Usage

```python
from strands_sql import run_sql_database

# Discover the schema
run_sql_database(
    action="schema_summary",
    connection_string="sqlite:///./local.db"
)

# Describe a specific table
run_sql_database(
    action="describe_table",
    table="users",
    connection_string="sqlite:///./local.db"
)

# Run a query (returns a markdown table by default)
run_sql_database(
    action="query",
    sql="SELECT * FROM orders WHERE amount > 100 LIMIT 20",
    connection_string="sqlite:///./local.db"
)
```

### With a Strands Agent

```python
from strands import Agent
from strands_sql import get_tool

agent = Agent(tools=[get_tool()])

# The agent decides when and how to invoke the tool
agent("List all tables in my database")
agent("Show me all orders above 100")
agent("How many users are there?")
```

> ⚠️ **Note**  
> Use `get_tool()` to properly register the tool with Strands. Passing `sql_database` directly will raise a `Tool 'sql_database' not found` error.

## Configuration

### Connection String

Pass it directly or set the `DATABASE_URL` environment variable:

```bash
export DATABASE_URL="postgresql://user:password@localhost:5432/mydb"
```

```python
run_sql_database(
    action="list_tables",
    connection_string="sqlite:///./local.db",
)
```

## Actions

| Action | Required params | Description |
|---|---|---|
| `list_tables` | — | List all tables and views |
| `describe_table` | `table` | Columns, types, PKs, FKs |
| `schema_summary` | — | Compact schema of all tables (ideal as LLM context) |
| `query` | `sql` | Run a SELECT statement |
| `execute` | `sql`, `read_only=False` | Run a write query (blocked by default) |

## Safety Options

```python
run_sql_database(
    action="query",
    sql="SELECT * FROM users",
    connection_string="sqlite:///./local.db",
    read_only=True,                      # Default: True — blocks INSERT/UPDATE/DELETE
    max_rows=500,                        # Default: 500 — caps result size
    timeout=30,                          # Default: 30s — kills hung queries
    allowed_tables=["users", "orders"],  # Allowlist
    blocked_tables=["secrets"],          # Blocklist
)
```

| Option | Default | Description |
|---|---|---|
| `read_only` | `True` | Blocks all write queries |
| `max_rows` | `500` | Maximum rows returned by `query` |
| `timeout` | `30` | Query timeout in seconds (1–300) |
| `output_format` | `markdown` | Output format: `markdown` or `json` |
| `allowed_tables` | `None` | Allowlist — only these tables are accessible |
| `blocked_tables` | `None` | Blocklist — these tables are never accessible |

## Output Formats

```python
# Markdown table (default — great for LLMs)
run_sql_database(
    action="query",
    sql="SELECT * FROM users",
    connection_string="sqlite:///./local.db",
    output_format="markdown"
)

# JSON array
run_sql_database(
    action="query",
    sql="SELECT * FROM users",
    connection_string="sqlite:///./local.db",
    output_format="json"
)
```

## Execute (Write Queries)

Write queries are blocked by default. To enable:

```python
run_sql_database(
    action="execute",
    sql="INSERT INTO users (name, age) VALUES ('Eve', 22)",
    connection_string="sqlite:///./local.db",
    read_only=False
)
```

## Development

```bash
git clone https://github.com/NithiN-1808/strands-sql
cd strands-sql
pip install -e ".[dev]"

# Run tests
pytest

# Run tests with coverage
pytest --cov=strands_sql --cov-report=term-missing

# Lint
ruff check src/ tests/

# Type check
mypy src/strands_sql/
```

## License

Apache 2.0