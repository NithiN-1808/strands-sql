# strands-tools-sql

A general-purpose SQL tool for [Strands Agents](https://strandsagents.com) — supporting PostgreSQL, MySQL, and SQLite via SQLAlchemy.

## Installation

```bash
# SQLite (no extra driver needed)
pip install strands-tools-sql

# PostgreSQL
pip install "strands-tools-sql[postgres]"

# MySQL
pip install "strands-tools-sql[mysql]"
```

## Quick Start

```python
from strands import Agent
from strands_sql import sql_database

agent = Agent(tools=[sql_database])

# Discover the schema
agent.tool.sql_database(action="schema_summary")

# Describe a specific table
agent.tool.sql_database(action="describe_table", table="users")

# Run a query (returns a markdown table by default)
agent.tool.sql_database(
    action="query",
    sql="SELECT * FROM orders WHERE amount > 100 LIMIT 20"
)
```

## Configuration

### Connection String

Pass it directly or set the `DATABASE_URL` environment variable:

```bash
export DATABASE_URL="postgresql://user:password@localhost:5432/mydb"
```

```python
agent.tool.sql_database(
    action="list_tables",
    connection_string="sqlite:///./local.db"
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
agent.tool.sql_database(
    action="query",
    sql="SELECT * FROM users",
    read_only=True,          # Default: True — blocks INSERT/UPDATE/DELETE
    max_rows=500,            # Default: 500 — caps result size
    timeout=30,              # Default: 30s — kills hung queries
    allowed_tables=["users", "orders"],  # Allowlist
    blocked_tables=["secrets"],          # Blocklist
)
```

| Option | Default | Description |
|---|---|---|
| `read_only` | `True` | Blocks all write queries |
| `max_rows` | `500` | Maximum rows returned by `query` |
| `timeout` | `30` | Query timeout in seconds (1–300) |
| `allowed_tables` | `None` | Allowlist — only these tables are accessible |
| `blocked_tables` | `None` | Blocklist — these tables are never accessible |

## Output Formats

```python
# Markdown table (default — great for LLMs)
agent.tool.sql_database(action="query", sql="SELECT * FROM users", output_format="markdown")

# JSON array
agent.tool.sql_database(action="query", sql="SELECT * FROM users", output_format="json")
```

## Development

```bash
git clone https://github.com/YOUR_USERNAME/strands-tools-sql
cd strands-tools-sql
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