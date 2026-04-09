"""Input/output models for strands-sql."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator


class SqlDatabaseInput(BaseModel):
    """Validated input for the sql_database tool."""

    action: Literal["list_tables", "describe_table", "schema_summary", "query", "execute"]

    sql: str | None = Field(None, description="SQL string for query/execute actions.")
    table: str | None = Field(None, description="Table name for describe_table action.")

    connection_string: str | None = Field(
        None, description="SQLAlchemy connection string. Falls back to DATABASE_URL env var."
    )

    read_only: bool = Field(True, description="Block write queries when True.")
    max_rows: int = Field(500, ge=1, le=10_000, description="Max rows returned by query.")
    timeout: int = Field(30, ge=1, le=300, description="Query timeout in seconds.")
    output_format: Literal["json", "markdown"] = Field(
        "markdown", description="Output format for query results."
    )

    allowed_tables: list[str] | None = Field(
        None, description="Allowlist — only these tables are accessible."
    )
    blocked_tables: list[str] | None = Field(
        None, description="Blocklist — these tables are never accessible."
    )

    @model_validator(mode="after")
    def check_sql_provided(self) -> SqlDatabaseInput:
        if self.action in ("query", "execute") and not self.sql:
            raise ValueError(f"'sql' is required when action='{self.action}'.")
        if self.action == "describe_table" and not self.table:
            raise ValueError("'table' is required when action='describe_table'.")
        return self
