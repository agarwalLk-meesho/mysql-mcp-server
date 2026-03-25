import json
import re
from typing import Any

import pymysql
import pymysql.cursors
from mcp.server.fastmcp import FastMCP

ALLOWED_STATEMENT_PATTERN = re.compile(
    r"^\s*(SELECT|SHOW|DESCRIBE|DESC|EXPLAIN)\b",
    re.IGNORECASE,
)

mcp = FastMCP("MySQL Server")

_connection: pymysql.connections.Connection | None = None


def _get_connection() -> pymysql.connections.Connection:
    if _connection is None or not _connection.open:
        raise RuntimeError(
            "Not connected. Call connect_mysql first with host, port, username, password, and db_name."
        )
    return _connection


def _format_rows(rows: list[dict[str, Any]], max_rows: int = 500) -> str:
    if not rows:
        return "Query returned 0 rows."
    truncated = rows[:max_rows]
    lines = [json.dumps(row, default=str) for row in truncated]
    result = "\n".join(lines)
    if len(rows) > max_rows:
        result += f"\n... truncated ({len(rows)} total rows, showing first {max_rows})"
    return result


@mcp.tool()
def connect_mysql(host: str, port: int, username: str, password: str, db_name: str) -> str:
    """Connect to a MySQL host with the given credentials and database name.
    Must be called before running any queries.

    Args:
        host: MySQL server hostname (e.g., 'db.example.com')
        port: MySQL server port (e.g., 3306)
        username: MySQL username
        password: MySQL password
        db_name: Name of the database to connect to
    """
    global _connection

    if _connection is not None and _connection.open:
        try:
            _connection.close()
        except Exception:
            pass

    try:
        _connection = pymysql.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=db_name,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10,
            read_timeout=30,
        )
        return f"Connected to {host}:{port}/{db_name} as {username}."
    except pymysql.Error as e:
        _connection = None
        return f"Connection failed: {e}"


@mcp.tool()
def disconnect() -> str:
    """Disconnect from the current MySQL connection."""
    global _connection
    if _connection is None:
        return "No active connection."
    try:
        _connection.close()
    except Exception:
        pass
    _connection = None
    return "Disconnected."


@mcp.tool()
def list_databases() -> str:
    """List all databases accessible with the current connection credentials."""
    conn = _get_connection()
    with conn.cursor() as cursor:
        cursor.execute("SHOW DATABASES")
        rows = cursor.fetchall()
    return "\n".join(row["Database"] for row in rows)


@mcp.tool()
def list_tables() -> str:
    """List all tables in the currently connected database."""
    conn = _get_connection()
    with conn.cursor() as cursor:
        cursor.execute("SHOW TABLES")
        rows = cursor.fetchall()
    if not rows:
        return "No tables found."
    key = list(rows[0].keys())[0]
    return "\n".join(row[key] for row in rows)


@mcp.tool()
def describe_table(table_name: str) -> str:
    """Show the schema (columns, types, keys) of a specific table.

    Args:
        table_name: Name of the table to describe
    """
    if not re.match(r"^\w+$", table_name):
        return "Invalid table name. Only alphanumeric characters and underscores are allowed."

    conn = _get_connection()
    with conn.cursor() as cursor:
        # table_name restricted to \w+ above — safe MySQL identifier, not a value placeholder.
        cursor.execute(f"DESCRIBE `{table_name}`")
        rows = cursor.fetchall()
    return _format_rows(rows)


@mcp.tool()
def run_query(query: str) -> str:
    """Execute a read-only SQL query (SELECT, SHOW, DESCRIBE, EXPLAIN only).
    Any write operations (INSERT, UPDATE, DELETE, DROP, etc.) will be rejected.

    Args:
        query: The SQL query to execute
    """
    if not ALLOWED_STATEMENT_PATTERN.match(query):
        return (
            "Query rejected: only SELECT, SHOW, DESCRIBE, and EXPLAIN statements are allowed. "
            "Write operations are not permitted."
        )

    conn = _get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
        return _format_rows(rows)
    except pymysql.Error as e:
        return f"Query error: {e}"
