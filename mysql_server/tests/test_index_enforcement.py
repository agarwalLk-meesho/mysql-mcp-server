import pathlib
import sys
import unittest

# Ensure the `src/` layout is importable when running `unittest` directly.
SRC_DIR = pathlib.Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

import mysql_mcp_server.server as mysql_server


class MockCursor:
    def __init__(self, parent: "MockConnection"):
        self._parent = parent
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query: str, params=None):
        q = query.strip()
        q_upper = q.upper()

        if "INFORMATION_SCHEMA.STATISTICS" in q_upper:
            self._parent.stats_execute_count += 1
            self._rows = [{"COLUMN_NAME": c} for c in sorted(self._parent.indexed_columns)]
            return

        if q_upper.startswith("EXPLAIN "):
            self._rows = [{"type": self._parent.explain_type}]
            return

        # For the actual SELECT query.
        self._rows = self._parent.select_rows

    def fetchall(self):
        return self._rows


class MockConnection:
    def __init__(self, indexed_columns: set[str], explain_type: str, select_rows: list[dict]):
        self.open = True
        self.indexed_columns = indexed_columns
        self.explain_type = explain_type
        self.select_rows = select_rows
        self.stats_execute_count = 0

    def cursor(self):
        return MockCursor(self)


def _reset_mysql_globals(conn: MockConnection, active_db: str = "testdb"):
    mysql_server._connection = conn
    mysql_server._active_db = active_db
    mysql_server._indexes_cache = {}


class IndexEnforcementTest(unittest.TestCase):
    def test_reject_non_indexed_predicate_column(self):
        conn = MockConnection(
            indexed_columns={"id"},
            explain_type="ref",
            select_rows=[{"id": 1, "name": "x"}],
        )
        _reset_mysql_globals(conn)

        result = mysql_server.run_query(
            "SELECT * FROM users WHERE name = 'x'",
            enforce_indexed_columns=True,
        )

        self.assertIn("Query rejected: predicate references non-indexed column(s).", result)
        self.assertIn("Non-indexed columns", result)
        self.assertIn("testdb.users: name", result)

    def test_allow_indexed_predicate_column(self):
        conn = MockConnection(
            indexed_columns={"id", "name"},
            explain_type="ref",
            select_rows=[{"id": 1, "name": "x"}],
        )
        _reset_mysql_globals(conn)

        result = mysql_server.run_query(
            "SELECT id, name FROM users WHERE name = 'x'",
            enforce_indexed_columns=True,
        )

        self.assertNotIn("Query rejected:", result)
        self.assertIn('"name": "x"', result)

    def test_indexed_columns_are_cached_between_runs(self):
        conn = MockConnection(
            indexed_columns={"id"},
            explain_type="ref",
            select_rows=[{"id": 1}],
        )
        _reset_mysql_globals(conn)

        query = "SELECT * FROM users WHERE name = 'x'"
        _ = mysql_server.run_query(query, enforce_indexed_columns=True)
        _ = mysql_server.run_query(query, enforce_indexed_columns=True)

        # Only the first run should query INFORMATION_SCHEMA.STATISTICS.
        self.assertEqual(conn.stats_execute_count, 1)

    def test_ambiguous_unmapped_columns_falls_back_to_explain_with_warning(self):
        conn = MockConnection(
            indexed_columns={"id", "user_id"},  # irrelevant: ambiguity should short-circuit indexed checks
            explain_type="ref",
            select_rows=[{"u_id": 1, "user_id": 1}],
        )
        _reset_mysql_globals(conn)

        # `id` is unqualified and appears in a query with multiple tables -> ambiguous mapping.
        result = mysql_server.run_query(
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE id = 5",
            enforce_indexed_columns=True,
        )

        self.assertIn("Index-column enforcement was best-effort", result)
        self.assertNotIn("Query rejected:", result)


if __name__ == "__main__":
    unittest.main()

