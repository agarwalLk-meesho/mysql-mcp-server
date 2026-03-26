import json
import os
import re
from typing import Any

import pymysql
import pymysql.cursors
from mcp.server.fastmcp import FastMCP
from sqlglot import exp, parse_one

ALLOWED_STATEMENT_PATTERN = re.compile(
    r"^\s*(SELECT|SHOW|DESCRIBE|DESC|EXPLAIN)\b",
    re.IGNORECASE,
)

SELECT_LIKE_START_PATTERN = re.compile(r"^\s*(WITH|SELECT)\b", re.IGNORECASE | re.DOTALL)
UNSAFE_TABLE_NAME_PATTERN = re.compile(r"[^\w]")

mcp = FastMCP("MySQL Server")

_connection: pymysql.connections.Connection | None = None
_active_db: str | None = None

# Cache of indexed columns per table (key: "db.table" or just "table" when db is unknown).
_indexes_cache: dict[str, set[str]] = {}


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


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


def _table_cache_key(schema: str | None, table: str) -> str:
    if schema:
        return f"{schema}.{table}"
    return table


def _extract_table_name(table_expr: exp.Table) -> tuple[str | None, str]:
    """Return (schema, table) for a sqlglot exp.Table expression."""
    schema: str | None = None
    # sqlglot uses "db" for schema / database in Table expressions for MySQL.
    db = table_expr.args.get("db")
    if db is not None:
        schema = str(db)
    return schema, str(table_expr.name)


def _load_indexed_columns_for_table(table_key: str, schema: str | None, table: str) -> set[str]:
    """Load indexed columns from INFORMATION_SCHEMA.STATISTICS and cache them."""
    if table_key in _indexes_cache:
        return _indexes_cache[table_key]

    if schema is None:
        # Without a schema we can't reliably query INFORMATION_SCHEMA for the correct DB.
        _indexes_cache[table_key] = set()
        return _indexes_cache[table_key]

    conn = _get_connection()
    cols: set[str] = set()
    with conn.cursor() as cursor:
        # Grouping avoids duplicates across composite indexes.
        cursor.execute(
            """
            SELECT DISTINCT COLUMN_NAME
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
              AND COLUMN_NAME IS NOT NULL
            """,
            (schema, table),
        )
        for row in cursor.fetchall():
            # DictCursor is used for this connection; COLUMN_NAME should exist.
            col = row.get("COLUMN_NAME") if hasattr(row, "get") else None
            if col:
                cols.add(str(col))

    _indexes_cache[table_key] = cols
    return cols


def _get_indexed_columns_for_reference(table_key: str) -> set[str]:
    indexed = _indexes_cache.get(table_key)
    if indexed is not None:
        return indexed

    # table_key is expected to be "schema.table" from our resolver.
    if "." not in table_key:
        _indexes_cache[table_key] = set()
        return _indexes_cache[table_key]

    schema, table = table_key.split(".", 1)
    return _load_indexed_columns_for_table(table_key=table_key, schema=schema, table=table)


def _is_select_like(query: str) -> bool:
    return bool(SELECT_LIKE_START_PATTERN.match(query))


def _explain_rejects_full_table_scan(conn: pymysql.connections.Connection, query: str) -> str | None:
    """Reject SELECT queries where EXPLAIN reports full table scan access type ALL."""
    q = query.strip().rstrip(";")
    if not _is_select_like(q):
        return None
    if re.match(r"^\s*EXPLAIN\b", q, re.IGNORECASE):
        # Don't apply enforcement to plans-of-plans.
        return None

    with conn.cursor() as cursor:
        cursor.execute(f"EXPLAIN {q}")
        rows = cursor.fetchall()

    for row in rows:
        # PyMySQL DictCursor typically returns lowercase keys; handle both cases.
        plan_type = row.get("type") if hasattr(row, "get") else None
        if plan_type is None and row:
            plan_type = next((row[k] for k in row if str(k).lower() == "type"), None)
        if plan_type is not None and str(plan_type).upper() == "ALL":
            return (
                "Query rejected: EXPLAIN shows a full table scan (access type ALL) for at least one table. "
                "Use WHERE/JOIN predicates that match an index (e.g. indexed equality/range), "
                "add an index, or narrow the query."
            )
    return None


def _extract_tables_and_aliases(ast: exp.Expression) -> tuple[dict[str, str], set[str], bool]:
    """Return (alias_to_table_key, table_keys, had_schema_refs).

    - alias_to_table_key maps qualifier (alias or table name) -> "schema.table"
    - table_keys is the set of "schema.table" referenced in FROM/JOIN.
    - had_schema_refs flags if query referenced an explicit schema != active schema; in that case mapping
      could be incomplete.
    """
    global _active_db
    if _active_db is None:
        return {}, set(), False

    alias_to_table_key: dict[str, str] = {}
    table_keys: set[str] = set()
    had_schema_refs = False

    for t in ast.find_all(exp.Table):
        schema_expr, table = _extract_table_name(t)
        schema = schema_expr or _active_db
        table_key = _table_cache_key(schema, table)
        table_keys.add(table_key)

        # If the query uses explicit schema and it differs, we can't guarantee index metadata.
        if schema_expr is not None and schema_expr != _active_db:
            had_schema_refs = True

        alias = t.alias
        if alias:
            alias_to_table_key[str(alias)] = table_key
        # Also allow unaliased qualification by table name.
        alias_to_table_key[table] = table_key

    return alias_to_table_key, table_keys, had_schema_refs


def _resolve_column_to_table_key(
    col: exp.Column,
    alias_to_table_key: dict[str, str],
    table_keys: set[str],
) -> tuple[str | None, bool]:
    """Return (table_key, ambiguous_or_unmapped)."""
    qualifier = col.table  # may be None
    if qualifier:
        qualifier_str = str(qualifier)
        if qualifier_str in alias_to_table_key:
            return alias_to_table_key[qualifier_str], False
        # Qualifier exists but isn't known; best-effort fail.
        return None, True

    # Unqualified column: ambiguous if multiple tables are present.
    if len(table_keys) == 1:
        return next(iter(table_keys)), False
    return None, True


def _extract_predicate_columns_by_table(ast: exp.Expression) -> tuple[dict[str, set[str]], bool]:
    """Extract columns referenced in WHERE and JOIN ... ON predicates.

    Returns (table_key -> referenced columns), and a boolean indicating ambiguity/unmapped columns.
    """
    alias_to_table_key, table_keys, had_schema_refs = _extract_tables_and_aliases(ast)
    referenced: dict[str, set[str]] = {}
    had_unmapped = had_schema_refs

    where_nodes = list(ast.find_all(exp.Where))
    on_nodes: list[exp.Expression] = []
    for j in ast.find_all(exp.Join):
        on = j.args.get("on")
        if on is not None:
            on_nodes.append(on)

    predicate_roots = []
    predicate_roots.extend(where_nodes)
    predicate_roots.extend(on_nodes)

    for root in predicate_roots:
        for c in root.find_all(exp.Column):
            table_key, ambiguous = _resolve_column_to_table_key(c, alias_to_table_key, table_keys)
            if ambiguous or table_key is None:
                had_unmapped = True
                continue
            referenced.setdefault(table_key, set()).add(str(c.name))

    return referenced, had_unmapped


def _reject_non_indexed_columns(
    ast: exp.Expression,
    require_indexed_columns: bool,
) -> tuple[str | None, bool]:
    """Return (error_message, had_ambiguous_mapping).

    We reject if any referenced predicate column is not present in the table's indexed columns.
    """
    if not require_indexed_columns:
        return None, False

    referenced_cols_by_table, had_unmapped = _extract_predicate_columns_by_table(ast)
    if had_unmapped:
        # Per plan: if we cannot map columns confidently, fall back to EXPLAIN-only enforcement.
        return None, True

    # If the query doesn't reference WHERE/JOIN columns, we have nothing to check (EXPLAIN still applies).
    if not referenced_cols_by_table:
        return None, False

    non_indexed: list[str] = []
    for table_key, cols in referenced_cols_by_table.items():
        indexed_cols = _get_indexed_columns_for_reference(table_key)
        bad = sorted([c for c in cols if c not in indexed_cols])
        if bad:
            non_indexed.append(f"{table_key}: {', '.join(bad)}")

    if non_indexed:
        return (
            "Query rejected: predicate references non-indexed column(s). "
            "For best performance, rewrite so WHERE/JOIN predicates use indexed columns.\n"
            f"Non-indexed columns (best-effort):\n- {'\n- '.join(non_indexed)}\n"
            "Suggestions:\n"
            "- Add an index on the referenced column(s), or\n"
            "- Change WHERE/JOIN predicates to use the indexed column(s) (e.g. indexed equality), or\n"
            "- Narrow the query by filtering on an indexed prefix column where applicable.",
            False,
        )

    return None, False


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
    global _connection, _active_db, _indexes_cache

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
        _active_db = db_name
        _indexes_cache = {}
        return f"Connected to {host}:{port}/{db_name} as {username}."
    except pymysql.Error as e:
        _connection = None
        _active_db = None
        _indexes_cache = {}
        return f"Connection failed: {e}"


@mcp.tool()
def disconnect() -> str:
    """Disconnect from the current MySQL connection."""
    global _connection, _active_db, _indexes_cache
    if _connection is None:
        return "No active connection."
    try:
        _connection.close()
    except Exception:
        pass
    _connection = None
    _active_db = None
    _indexes_cache = {}
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


def _parse_sql_ast(query: str) -> exp.Expression | None:
    try:
        return parse_one(query, read="mysql")
    except Exception:
        return None


def _enforcement_enabled(explicit: bool) -> bool:
    return explicit or _env_truthy("MCP_MYSQL_ENFORCE_INDEXED_COLUMNS")


def _index_policy_enforcement_result(
    conn: pymysql.connections.Connection,
    query: str,
) -> tuple[str | None, str | None]:
    """Return (warning_prefix, rejection_error).

    - warning_prefix is included in the final response when we enforce via best-effort parsing
      (e.g. ambiguous/unmapped columns).
    - rejection_error, when not None, means the query must be rejected.
    """
    if not _is_select_like(query):
        return None, None

    ast = _parse_sql_ast(query)
    warning_prefix: str | None = None
    if ast is not None:
        err, ambiguous = _reject_non_indexed_columns(ast, require_indexed_columns=True)
        if err is not None:
            return None, err
        if ambiguous:
            warning_prefix = (
                "Index-column enforcement was best-effort: some predicate columns could not be "
                "confidently mapped to a table. Proceeding with EXPLAIN-only full scan check."
            )
    else:
        warning_prefix = (
            "Index-column enforcement fell back to EXPLAIN-only because SQL parsing of WHERE/JOIN "
            "predicates failed."
        )

    explain_err = _explain_rejects_full_table_scan(conn, query)
    if explain_err is not None:
        return None, explain_err

    return warning_prefix, None


@mcp.tool()
def run_query(query: str, enforce_indexed_columns: bool = False) -> str:
    """Execute a read-only SQL query (SELECT, SHOW, DESCRIBE, EXPLAIN only).
    Any write operations (INSERT, UPDATE, DELETE, DROP, etc.) will be rejected.

    When enforcement is enabled, for SELECT queries:
    - Validate WHERE/JOIN predicate columns against INFORMATION_SCHEMA.STATISTICS indexes
      and reject queries referencing non-indexed predicate columns (best-effort mapping).
    - Also run EXPLAIN and reject if MySQL reports a full table scan (access type ALL).

    Args:
        query: The SQL query to execute
        enforce_indexed_columns: Overrides env `MCP_MYSQL_ENFORCE_INDEXED_COLUMNS` (if set).
    """
    if not ALLOWED_STATEMENT_PATTERN.match(query):
        return (
            "Query rejected: only SELECT, SHOW, DESCRIBE, and EXPLAIN statements are allowed. "
            "Write operations are not permitted."
        )

    enforcement = _enforcement_enabled(enforce_indexed_columns)
    conn = _get_connection()

    warning_prefix: str | None = None
    if enforcement:
        warning_prefix, rejection_error = _index_policy_enforcement_result(conn, query)
        if rejection_error is not None:
            return rejection_error

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
        formatted = _format_rows(rows)
        if warning_prefix:
            return f"{warning_prefix}\n\n{formatted}"
        return formatted
    except pymysql.Error as e:
        return f"Query error: {e}"
